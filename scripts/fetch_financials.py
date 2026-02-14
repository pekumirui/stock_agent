"""
決算データ取得バッチ - EDINET API

EDINETから有価証券報告書・四半期報告書を取得し、
決算情報をDBに保存する

EDINET APIの利用には事前登録が必要:
https://disclosure.edinet-fsa.go.jp/

使用方法:
    python fetch_financials.py                      # 直近の決算書類を取得
    python fetch_financials.py --days 30            # 過去30日分
    python fetch_financials.py --ticker 7203        # 特定銘柄のみ
    python fetch_financials.py --doc-id S100XXXXX   # 特定書類を処理
    python fetch_financials.py --include-quarterly --days 1095  # Q1/Q3初期投入（過去3年）
"""
import argparse
import os
import requests
import zipfile
import io
import sys
import shutil
import tempfile
import time
import re
import json
import unicodedata
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Optional, Dict, Any, List
import xml.etree.ElementTree as ET

# XBRLPライブラリのインポート
BASE_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(BASE_DIR / "lib"))
from xbrlp import Parser, Fact, QName, FileLoader

from db_utils import (
    get_connection, get_all_tickers, insert_financial,
    log_batch_start, log_batch_end,
    get_edinet_ticker_map, get_processed_doc_ids
)


# EDINET API エンドポイント
EDINET_API_BASE = "https://api.edinet-fsa.go.jp/api/v2"

# 書類種別コード（EDINET API v2準拠）
DOC_TYPE_CODES = {
    '120': '有価証券報告書',
    '130': '訂正有価証券報告書',
    '135': '確認書',
    '140': '四半期報告書',
    '160': '半期報告書',
    '180': '臨時報告書',
    '220': '自己株券買付状況報告書',
}

# XBRLP用: QName local_name → DBフィールドのマッピング（日本基準）
XBRL_FACT_MAPPING = {
    # 売上高
    'NetSales': 'revenue',
    'Revenue': 'revenue',
    'OperatingRevenue': 'revenue',
    # 売上高（業種別バリエーション - jppfs_cor）
    'OperatingRevenue1': 'revenue',                              # 鉄道・バス・不動産・通信
    'OperatingRevenue2': 'revenue',                              # 保険業
    'NetSalesOfCompletedConstructionContracts': 'revenue',       # 建設業
    'NetSalesOfCompletedConstructionContractsCNS': 'revenue',    # 建設業（連結）
    'NetSalesAndOperatingRevenue': 'revenue',                    # 電力・ガス等
    'NetSalesAndOperatingRevenue2': 'revenue',                   # 一部特殊業種
    'BusinessRevenue': 'revenue',                                # 商社・サービス
    'OperatingRevenueELE': 'revenue',                            # 電力業
    'ShippingBusinessRevenueWAT': 'revenue',                     # 海運業
    'OperatingRevenueSEC': 'revenue',                            # 証券業
    'OperatingRevenueSPF': 'revenue',                            # 特定金融業
    'OrdinaryIncomeBNK': 'revenue',                              # 銀行業（経常収益）
    'OrdinaryIncomeINS': 'revenue',                              # 保険業（経常収益）
    'OperatingIncomeINS': 'revenue',                             # 保険業（営業収益）
    'TotalOperatingRevenue': 'revenue',                          # 営業収益合計
    'OperatingRevenueINV': 'revenue',                              # 投資業
    'OperatingRevenueIVT': 'revenue',                              # IVT業
    'OperatingRevenueCMD': 'revenue',                              # CMD業
    # 売上高（有価証券報告書 経営指標サマリー - jpcrp_cor）
    'NetSalesSummaryOfBusinessResults': 'revenue',
    'OperatingRevenue1SummaryOfBusinessResults': 'revenue',
    'OperatingRevenue2SummaryOfBusinessResults': 'revenue',
    'NetSalesOfCompletedConstructionContractsSummaryOfBusinessResults': 'revenue',
    'NetSalesAndOperatingRevenueSummaryOfBusinessResults': 'revenue',
    'BusinessRevenueSummaryOfBusinessResults': 'revenue',
    'OrdinaryIncomeBNKSummaryOfBusinessResults': 'revenue',
    'OrdinaryIncomeINSSummaryOfBusinessResults': 'revenue',      # 保険業（有報サマリー）
    'RevenueIFRSSummaryOfBusinessResults': 'revenue',            # IFRS企業の有報
    'RevenuesUSGAAPSummaryOfBusinessResults': 'revenue',         # US-GAAP企業の有報
    # 売上総利益
    'GrossProfit': 'gross_profit',
    'GrossProfitOnCompletedConstructionContracts': 'gross_profit',    # 建設業
    'GrossProfitOnCompletedConstructionContractsCNS': 'gross_profit', # 建設業（連結）
    # 売上総利益（業種別バリエーション）
    'NetOperatingRevenueSEC': 'gross_profit',                        # 第一種金融商品取引業（純営業収益）
    'OperatingGrossProfit': 'gross_profit',                          # 一般商工業（営業総利益）
    'OperatingGrossProfitWAT': 'gross_profit',                       # 海運業（営業総利益）
    # 営業利益
    'OperatingIncome': 'operating_income',
    'OperatingProfit': 'operating_income',
    # 営業利益（IFRS有報/半期報サマリー - jpcrp_cor）
    'OperatingProfitLossIFRSSummaryOfBusinessResults': 'operating_income',
    # 経常利益
    'OrdinaryIncome': 'ordinary_income',
    'OrdinaryProfit': 'ordinary_income',
    # 経常利益（IFRS有報/半期報サマリー - jpcrp_cor、税引前利益）
    'ProfitLossBeforeTaxIFRSSummaryOfBusinessResults': 'ordinary_income',
    # 経常利益（US-GAAP有報/半期報サマリー - jpcrp_cor、税引前利益）
    'ProfitLossBeforeTaxUSGAAPSummaryOfBusinessResults': 'ordinary_income',
    # 当期純利益
    'ProfitLoss': 'net_income',
    'NetIncome': 'net_income',
    'ProfitLossAttributableToOwnersOfParent': 'net_income',
    # 当期純利益（IFRS有報/半期報サマリー - jpcrp_cor）
    'ProfitLossAttributableToOwnersOfParentIFRSSummaryOfBusinessResults': 'net_income',
    # 当期純利益（US-GAAP有報/半期報サマリー - jpcrp_cor）
    'NetIncomeLossAttributableToOwnersOfParentUSGAAPSummaryOfBusinessResults': 'net_income',
    # EPS
    'BasicEarningsLossPerShare': 'eps',
    'EarningsPerShare': 'eps',
    'BasicEarningsLossPerShareSummaryOfBusinessResults': 'eps',  # EDINET有報・半期報
    'DilutedEarningsPerShareSummaryOfBusinessResults': 'eps',    # EDINET有報・半期報（希薄化後）
    'BasicEarningsLossPerShareUSGAAPSummaryOfBusinessResults': 'eps',     # US-GAAP有報・半期報
    'DilutedEarningsLossPerShareUSGAAPSummaryOfBusinessResults': 'eps',   # US-GAAP有報・半期報（希薄化後）
}

# IFRS用マッピング（IFRS採用企業向け）
XBRL_FACT_MAPPING_IFRS = {
    # 売上高
    'Revenue': 'revenue',
    'SalesIFRS': 'revenue',  # TDnet用
    'RevenueFromContractsWithCustomers': 'revenue',   # IFRS 15（顧客との契約から生じる収益）
    'RevenueIFRS': 'revenue',                         # EDINET IFRS対応
    'Revenue2IFRS': 'revenue',                         # 一部IFRS企業
    'NetSalesAndOperatingRevenueIFRS': 'revenue',      # IFRS営業収益
    'OperatingRevenueIFRS': 'revenue',                 # IFRS営業収益
    'TotalNetRevenuesIFRS': 'revenue',                 # IFRS合計収益
    # 売上総利益
    'GrossProfit': 'gross_profit',
    'GrossProfitIFRS': 'gross_profit',  # jpigp_cor用（Attachment）
    # 営業利益（IFRSでは複数パターンあり）
    'ProfitLossFromOperatingActivities': 'operating_income',
    'OperatingProfitLoss': 'operating_income',
    'OperatingProfitLossIFRS': 'operating_income',             # EDINET jpigp_cor用
    'OperatingIncomeIFRS': 'operating_income',                 # TDnet用
    # 税引前利益（IFRSに経常利益はない）
    'ProfitLossBeforeTax': 'ordinary_income',
    'ProfitLossBeforeTaxIFRS': 'ordinary_income',              # EDINET jpigp_cor用
    'ProfitBeforeTaxIFRS': 'ordinary_income',                  # TDnet用
    # 純利益
    'ProfitLossAttributableToOwnersOfParent': 'net_income',
    'ProfitLoss': 'net_income',
    'ProfitLossAttributableToOwnersOfParentIFRS': 'net_income', # EDINET jpigp_cor用
    'ProfitAttributableToOwnersOfParentIFRS': 'net_income',    # TDnet用
    # EPS
    'BasicEarningsLossPerShare': 'eps',
    'DilutedEarningsLossPerShare': 'eps',
    'BasicEarningsPerShareIFRS': 'eps',  # TDnet用
    'BasicEarningsLossPerShareIFRS': 'eps',  # EDINET jpigp_cor用
    'BasicEarningsLossPerShareIFRSSummaryOfBusinessResults': 'eps',    # EDINET有報
    'DilutedEarningsLossPerShareIFRSSummaryOfBusinessResults': 'eps',  # EDINET有報（希薄化後）
    # TDnet EPS（tse-ed-t / jpigp_cor名前空間）
    'NetIncomePerShare': 'eps',                         # TDnet日本基準EPS（tse-ed-t）
    'DilutedNetIncomePerShare': 'eps',                  # TDnet日本基準 希薄化後EPS（tse-ed-t）
    'DilutedEarningsPerShareIFRS': 'eps',               # TDnet IFRS 希薄化後EPS（tse-ed-t）
    'DilutedEarningsLossPerShareIFRS': 'eps',           # IFRS 希薄化後EPS（jpigp_cor）
    'NetIncomePerShareUS': 'eps',                       # TDnet US-GAAP EPS（tse-ed-t）
    'BasicAndDilutedEarningsLossPerShareIFRS': 'eps',   # IFRS 基本/希薄化統合EPS（jpigp_cor）
    # TDnet 売上（tse-ed-t / jpigp_cor名前空間）
    'OperatingRevenues': 'revenue',          # TDnet営業収益（tse-ed-t）
    'OrdinaryRevenuesBK': 'revenue',         # TDnet銀行業経常収益（tse-ed-t）
    'OrdinaryRevenuesIN': 'revenue',         # TDnet保険業経常収益（tse-ed-t）
    'OperatingRevenuesSE': 'revenue',        # TDnetサービス業営業収益（tse-ed-t）
    'NetSalesIFRS': 'revenue',               # TDnet IFRS売上高（tse-ed-t/jpigp_cor）
    'OperatingRevenuesIFRS': 'revenue',      # TDnet IFRS営業収益（tse-ed-t）
    'NetSalesUS': 'revenue',                 # TDnet US-GAAP売上高（tse-ed-t）
    # TDnet 純利益（tse-ed-t / jpigp_cor名前空間）
    'ProfitAttributableToOwnersOfParent': 'net_income',  # TDnet親会社帰属利益（tse-ed-t）※Lossなし版
    'ProfitLossIFRS': 'net_income',                       # IFRS純利益（jpigp_cor）
    'ProfitIFRS': 'net_income',                            # TDnet IFRS利益（tse-ed-t）
    'NetIncomeUS': 'net_income',                           # TDnet US-GAAP純利益（tse-ed-t）
    # TDnet 営業利益（tse-ed-t名前空間）
    'OperatingIncomeUS': 'operating_income',  # TDnet US-GAAP営業利益（tse-ed-t）
}

# レガシーパーサー用: 財務項目のXBRLタグ（日本基準）
XBRL_TAGS_LEGACY = {
    'revenue': [
        'jppfs_cor:NetSales',
        'jppfs_cor:Revenue',
        'jppfs_cor:OperatingRevenue',
    ],
    'gross_profit': [
        'jppfs_cor:GrossProfit',
    ],
    'operating_income': [
        'jppfs_cor:OperatingIncome',
        'jppfs_cor:OperatingProfit',
    ],
    'ordinary_income': [
        'jppfs_cor:OrdinaryIncome',
        'jppfs_cor:OrdinaryProfit',
    ],
    'net_income': [
        'jppfs_cor:ProfitLoss',
        'jppfs_cor:NetIncome',
        'jppfs_cor:ProfitLossAttributableToOwnersOfParent',
    ],
    'eps': [
        'jppfs_cor:BasicEarningsLossPerShare',
        'jppfs_cor:EarningsPerShare',
        'jpcrp_cor:BasicEarningsLossPerShareSummaryOfBusinessResults',  # EDINET有報
    ],
}

# jppfs名前空間パターン（日本基準）
JPPFS_NAMESPACE_PATTERNS = [
    'jppfs_cor',
    'jpcrp_cor',  # 企業内容等開示タクソノミ（EPSなどの開示項目）
    'http://disclosure.edinet-fsa.go.jp/taxonomy/jppfs',
]

# IFRS名前空間パターン
IFRS_NAMESPACE_PATTERNS = [
    'ifrs-full',
    'ifrs_cor',
    'jpcif_cor',  # 日本IFRSタクソノミ
    'jpigp_cor',  # 日本IFRS汎用タクソノミ
    'tse-ed-t',   # 東証電子開示タクソノミ（TDnet用）
    'http://xbrl.ifrs.org/taxonomy',
]

# XBRL cache ディレクトリ
XBRL_CACHE_DIR = BASE_DIR / "data" / "xbrl_cache"

# FileLoaderインスタンス（モジュールレベルで共有）
_file_loader = FileLoader(cache_dir=XBRL_CACHE_DIR, ignore_failure=True)


class EdinetClient:
    """EDINET APIクライアント"""

    def __init__(self, api_key: str = None):
        self.api_key = api_key
        self.session = requests.Session()

    def get_document_list(self, date: str, doc_type: str = None) -> list:
        """
        指定日の書類一覧を取得

        Args:
            date: 取得日（YYYY-MM-DD）
            doc_type: 書類種別コード（120=有報, 130=四半期報告書）
        """
        url = f"{EDINET_API_BASE}/documents.json"
        params = {
            'date': date,
            'type': 2,  # 2=メタデータのみ
        }
        if self.api_key:
            params['Subscription-Key'] = self.api_key

        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            documents = data.get('results', [])

            # 書類種別でフィルタ
            if doc_type:
                documents = [d for d in documents if d.get('docTypeCode') == doc_type]

            return documents

        except Exception as e:
            print(f"  [ERROR] 書類一覧取得失敗 ({date}): {e}")
            return []

    def download_document(self, doc_id: str, output_dir: Path = None) -> Optional[Path]:
        """
        書類をダウンロード（ZIP形式）

        Args:
            doc_id: 書類管理番号
            output_dir: 出力ディレクトリ
        """
        url = f"{EDINET_API_BASE}/documents/{doc_id}"
        params = {
            'type': 1,  # 1=XBRL含むZIP
        }
        if self.api_key:
            params['Subscription-Key'] = self.api_key

        try:
            response = self.session.get(url, params=params, timeout=60)
            response.raise_for_status()

            if output_dir:
                output_dir.mkdir(parents=True, exist_ok=True)
                zip_path = output_dir / f"{doc_id}.zip"
                with open(zip_path, 'wb') as f:
                    f.write(response.content)
                return zip_path
            else:
                return response.content

        except Exception as e:
            print(f"  [ERROR] 書類ダウンロード失敗 ({doc_id}): {e}")
            return None


def extract_edinet_zip(zip_content: bytes) -> Optional[List[Path]]:
    """
    EDINETのZIPを一時ディレクトリに展開し、
    iXBRLファイルのパスリストを返す

    EDINET ZIP構造:
      XBRL/
        PublicDoc/
          manifest.xml (or manifest_*.xml)
          *.htm / *.html (iXBRLファイル)
          *.xbrl (旧形式)
          *.xsd
          *_lab.xml, *_pre.xml, *_cal.xml (linkbase)

    TDnet ZIP構造:
      XBRLData/
        Summary/
          tse-acedifsm-*.htm (決算短信サマリー - 財務ハイライト)
        Attachment/
          *.htm (詳細財務諸表: *pl*=P/L, *bs*=B/S等)
          manifest.xml

    Returns:
        展開されたファイルのパスリスト。
        呼び出し側でtemp_dirの削除が必要。
    """
    temp_dir = Path(tempfile.mkdtemp(prefix="edinet_"))
    try:
        with zipfile.ZipFile(io.BytesIO(zip_content)) as zf:
            zf.extractall(temp_dir)

        # デバッグ: ZIP内のファイル構造を出力（最初の10ファイル）
        all_files = list(temp_dir.rglob("*"))[:10]
        print(f"    [DEBUG] ZIP内のファイル（最初10件）: {[str(f.relative_to(temp_dir)) for f in all_files if f.is_file()]}")

        result_files = []

        # TDnet対応: SummaryディレクトリのiXBRL（決算短信サマリー）
        for summary_htm in sorted(temp_dir.rglob("*ixbrl.htm")):
            if "Summary" in summary_htm.parts:
                print(f"    [DEBUG] TDnet Summaryファイル発見: {summary_htm.relative_to(temp_dir)}")
                result_files.append(summary_htm)

        # TDnet対応: Attachment内の損益計算書（P/L）でgross_profitを取得
        # ファイル名パターン: J-GAAP=acedjppl, IFRS=acifrspl, US-GAAP=acusgpl
        for attachment_htm in sorted(temp_dir.rglob("*pl*ixbrl.htm")):
            if "Attachment" in attachment_htm.parts:
                print(f"    [DEBUG] TDnet P/Lファイル発見: {attachment_htm.relative_to(temp_dir)}")
                result_files.append(attachment_htm)

        # TDnetファイルが見つかった場合はそれを返す
        if result_files:
            return result_files

        # EDINET: manifestファイルを探す（iXBRL形式）
        for manifest in sorted(temp_dir.rglob("manifest*.xml")):
            print(f"    [DEBUG] manifestファイル発見: {manifest.relative_to(temp_dir)}")
            # PublicDoc制約を緩和（TDnet対応）
            if "PublicDoc" in str(manifest) or ("XBRL" in str(manifest) and "AuditDoc" not in str(manifest)):
                return [manifest]

        # manifestがない場合は.xbrlファイルを探す（旧形式フォールバック）
        for xbrl_file in sorted(temp_dir.rglob("*.xbrl")):
            print(f"    [DEBUG] XBRLファイル発見: {xbrl_file.relative_to(temp_dir)}")
            # PublicDoc制約を緩和（TDnet対応）
            if "PublicDoc" in str(xbrl_file) or ("XBRL" in str(xbrl_file) and "AuditDoc" not in str(xbrl_file)):
                return [xbrl_file]

        print(f"    [WARN] ZIPにXBRL関連ファイルが見つかりません")
        shutil.rmtree(temp_dir, ignore_errors=True)
        return None
    except Exception as e:
        print(f"  [ERROR] ZIP展開失敗: {e}")
        shutil.rmtree(temp_dir, ignore_errors=True)
        return None


def _is_jppfs_namespace(qname: QName) -> bool:
    """QNameがjppfs_cor/jpcrp_cor名前空間（日本基準）に属するか判定"""
    if qname.prefix:
        for pattern in JPPFS_NAMESPACE_PATTERNS:
            if pattern in qname.prefix:
                return True
    if qname.namespace_uri:
        for pattern in JPPFS_NAMESPACE_PATTERNS:
            if pattern in qname.namespace_uri:
                return True
    return False


def _is_ifrs_namespace(qname: QName) -> bool:
    """QNameがIFRS名前空間に属するか判定"""
    if qname.prefix:
        for pattern in IFRS_NAMESPACE_PATTERNS:
            if pattern in qname.prefix:
                return True
    if qname.namespace_uri:
        for pattern in IFRS_NAMESPACE_PATTERNS:
            if pattern in qname.namespace_uri:
                return True
    return False


def _is_supported_namespace(qname: QName) -> bool:
    """サポートされた名前空間（日本基準 or IFRS）か判定"""
    return _is_jppfs_namespace(qname) or _is_ifrs_namespace(qname)


def _is_current_period_context(context_ref: str) -> bool:
    """当期のコンテキストか判定（有報: Current*, 半期報: Interim*）"""
    ctx = context_ref.lower()
    if 'prior' in ctx:
        return False
    if 'current' in ctx or 'interim' in ctx:
        return True
    return False


def _extract_fiscal_end_date_from_xbrl(ixbrl_paths: list) -> Optional[str]:
    """iXBRLのContext要素から決算期末日を抽出する。

    CurrentYearInstant(期末時点)のinstantを優先的に取得。
    見つからない場合はCurrentYearDurationのendDateにフォールバック。

    Returns:
        決算期末日 (例: "2026-03-31") or None
    """
    for ixbrl_path in ixbrl_paths:
        xbrli_ns = None

        for event, elem in ET.iterparse(str(ixbrl_path), events=["start-ns", "end"]):
            if event == "start-ns":
                prefix, uri = elem
                if uri == "http://www.xbrl.org/2003/instance":
                    xbrli_ns = uri
            elif event == "end" and xbrli_ns:
                context_tag = f"{{{xbrli_ns}}}context"
                if elem.tag == context_tag:
                    ctx_id = elem.get("id", "")
                    # CurrentYearInstant → 期末時点（最も正確）
                    if "CurrentYear" in ctx_id and "Instant" in ctx_id:
                        period = elem.find(f"{{{xbrli_ns}}}period")
                        if period is not None:
                            # scenario付き（ディメンション指定）は除外
                            scenario = elem.find(f"{{{xbrli_ns}}}scenario")
                            if scenario is not None:
                                continue
                            instant = period.find(f"{{{xbrli_ns}}}instant")
                            if instant is not None and instant.text:
                                return instant.text

        # Instant未発見時のフォールバック: CurrentYearDurationのendDate
        for event, elem in ET.iterparse(str(ixbrl_path), events=["start-ns", "end"]):
            if event == "start-ns":
                prefix, uri = elem
                if uri == "http://www.xbrl.org/2003/instance":
                    xbrli_ns = uri
            elif event == "end" and xbrli_ns:
                context_tag = f"{{{xbrli_ns}}}context"
                if elem.tag == context_tag:
                    ctx_id = elem.get("id", "")
                    if "CurrentYear" in ctx_id and "Duration" in ctx_id:
                        scenario = elem.find(f"{{{xbrli_ns}}}scenario")
                        if scenario is not None:
                            continue
                        period = elem.find(f"{{{xbrli_ns}}}period")
                        if period is not None:
                            end_date = period.find(f"{{{xbrli_ns}}}endDate")
                            if end_date is not None and end_date.text:
                                return end_date.text

    return None


def parse_ixbrl_financials(ixbrl_paths) -> Dict[str, Any]:
    """
    XBRLPを使ってiXBRLから財務データを抽出

    Args:
        ixbrl_paths: iXBRLファイルのパス（Path）またはパスのリスト（List[Path]）
                     manifest.xmlパスも可（従来互換）

    Returns:
        {'revenue': 123456.0, 'operating_income': 12345.0, ...}
    """
    parser = Parser(file_loader=_file_loader)

    # リストで渡された場合（複数ファイル対応）
    if isinstance(ixbrl_paths, list):
        print(f"    [DEBUG] 複数iXBRLファイルを使用: {[p.name for p in ixbrl_paths]}")
        parser.ixbrl_files = ixbrl_paths
    # .htm/.htmlファイルを直接渡された場合
    elif ixbrl_paths.suffix.lower() in ['.htm', '.html']:
        print(f"    [DEBUG] 直接iXBRLファイルを使用: {ixbrl_paths.name}")
        parser.ixbrl_files = [ixbrl_paths]
    else:
        # manifest.xmlの場合（従来処理）
        try:
            parser.prepare_ixbrl(ixbrl_paths)
        except ValueError:
            # manifestからiXBRLファイルが見つからない場合、
            # PublicDoc内の.htm/.htmlファイルを直接探す
            public_doc_dir = ixbrl_paths.parent
            htm_files = list(public_doc_dir.glob("*.htm")) + list(public_doc_dir.glob("*.html"))
            if htm_files:
                parser.ixbrl_files = htm_files
            else:
                print(f"    [WARN] iXBRLファイルが見つかりません")
                return {}

    financials = {}
    unmatched_elements = set()  # 未マッチ要素の収集（診断用）

    try:
        for fact in parser.load_facts():
            # サポートされた名前空間のFactのみ対象（日本基準 or IFRS）
            if not _is_supported_namespace(fact.qname):
                continue

            # 当期データのみ
            if not _is_current_period_context(fact.context_ref):
                continue

            # マッピング対象か確認（日本基準 → IFRS の順で検索）
            db_field = XBRL_FACT_MAPPING.get(fact.qname.local_name)
            if not db_field:
                db_field = XBRL_FACT_MAPPING_IFRS.get(fact.qname.local_name)
            if not db_field:
                unmatched_elements.add(f"{fact.qname.prefix or '?'}:{fact.qname.local_name}")
                continue

            # 既にセット済みならスキップ（最初に見つかった値を優先）
            if db_field in financials:
                continue

            # 値を取得
            value = fact.value
            if value is None:
                continue

            # Decimal→float変換、EPS以外は百万円単位に変換
            if isinstance(value, Decimal):
                if db_field != 'eps':
                    value = float(value / 1_000_000)
                else:
                    value = float(value)
            else:
                try:
                    value = float(value)
                    if db_field != 'eps':
                        value = value / 1_000_000
                except (ValueError, TypeError):
                    continue

            financials[db_field] = value
    except Exception as e:
        print(f"    [ERROR] iXBRLパース失敗: {e}")
        return {}

    if unmatched_elements:
        # P/L関連要素を優先表示（アルファベット順だとO*,P*が埋もれるため）
        pl_prefixes = ('Operating', 'Profit', 'Net', 'Revenue', 'Gross', 'Basic', 'Diluted', 'Earnings', 'Ordinary')
        pl_elements = sorted(e for e in unmatched_elements if any(e.split(':')[-1].startswith(p) for p in pl_prefixes))
        other_count = len(unmatched_elements) - len(pl_elements)
        display = pl_elements[:20]
        print(f"    [DEBUG] 未マッチXBRL要素 ({len(unmatched_elements)}件、P/L関連{len(pl_elements)}件): "
              f"{', '.join(display)}"
              + (f" ... 他{other_count}件" if other_count > 0 else ""))

    # iXBRLのContext要素から決算期末日を取得
    if parser.ixbrl_files:
        fiscal_end = _extract_fiscal_end_date_from_xbrl(parser.ixbrl_files)
        if fiscal_end:
            financials['fiscal_end_date'] = fiscal_end

    return financials


def _parse_xbrl_legacy(xbrl_content: str) -> Dict[str, Any]:
    """
    レガシーXBRLパーサー（旧形式の.xbrlファイル用フォールバック）

    Returns:
        {'revenue': 123456.0, 'operating_income': 12345.0, ...}
    """
    financials = {}

    try:
        root = ET.fromstring(xbrl_content)
        namespaces = dict([node for _, node in ET.iterparse(io.StringIO(xbrl_content), events=['start-ns'])])

        for field, tags in XBRL_TAGS_LEGACY.items():
            for tag in tags:
                prefix, element = tag.split(':')

                for ns_prefix, ns_uri in namespaces.items():
                    if prefix in ns_prefix or prefix.lower() in ns_uri.lower():
                        xpath = f".//{{{ns_uri}}}{element}"
                        elements = root.findall(xpath)

                        for elem in elements:
                            context = elem.get('contextRef', '')
                            if 'Current' in context or 'current' in context:
                                try:
                                    value = float(elem.text)
                                    unit_ref = elem.get('unitRef', '')
                                    if 'JPY' in unit_ref.upper():
                                        value = value / 1_000_000
                                    financials[field] = value
                                    break
                                except (ValueError, TypeError):
                                    continue

                if field in financials:
                    break

        return financials

    except Exception as e:
        print(f"  [ERROR] レガシーXBRLパース失敗: {e}")
        return {}


def edinet_code_to_ticker(edinet_code: str) -> Optional[str]:
    """EDINETコードから証券コードを取得"""
    with get_connection() as conn:
        cursor = conn.execute(
            "SELECT ticker_code FROM companies WHERE edinet_code = ?",
            (edinet_code,)
        )
        row = cursor.fetchone()
        return row['ticker_code'] if row else None


def ticker_to_edinet_code(ticker_code: str) -> Optional[str]:
    """証券コードからEDINETコードを取得"""
    with get_connection() as conn:
        cursor = conn.execute(
            "SELECT edinet_code FROM companies WHERE ticker_code = ?",
            (ticker_code,)
        )
        row = cursor.fetchone()
        return row['edinet_code'] if row else None


WAREKI_MAP = {
    '令和': 2018,  # 令和N年 = 2018 + N
    '平成': 1988,  # 平成N年 = 1988 + N
}


def _wareki_to_seireki(text: str) -> str:
    """和暦表記を西暦に変換する。

    例: "令和7年12月期" → "2025年12月期"
    """
    for era, offset in WAREKI_MAP.items():
        match = re.search(rf'{era}(\d{{1,2}})年', text)
        if match:
            year = offset + int(match.group(1))
            return text[:match.start()] + f'{year}年' + text[match.end():]
    return text


def _detect_edinet_quarter(doc_info: dict) -> tuple[str, str]:
    """
    EDINET書類情報からfiscal_yearとfiscal_quarterを判定

    docDescriptionから「第N四半期」「YYYY年M月期」等のパターンを抽出。
    フォールバックとしてperiodEndの年を使用。

    Returns:
        (fiscal_year: str, fiscal_quarter: str)
    """
    doc_type = doc_info.get('docTypeCode', '')
    period_end = doc_info.get('periodEnd', '')
    doc_description = doc_info.get('docDescription', '')

    # 全角数字→半角数字に正規化（防御的: EDINETは通常半角だが念のため）
    normalized_desc = unicodedata.normalize('NFKC', doc_description)
    normalized_desc = _wareki_to_seireki(normalized_desc)

    # fiscal_quarter の判定
    if doc_type == '120':
        fiscal_quarter = 'FY'
    elif doc_type == '160':
        fiscal_quarter = 'Q2'
    elif doc_type == '140':
        # 四半期報告書: docDescriptionから「第N四半期」を抽出
        q_match = re.search(r'第([1-3])四半期', normalized_desc)
        if q_match:
            fiscal_quarter = f'Q{q_match.group(1)}'
        else:
            fiscal_quarter = 'Q1'  # フォールバック
            print(f"    [WARN] 四半期番号が判定できずQ1にフォールバック: {doc_description}")
    else:
        fiscal_quarter = 'FY'

    # fiscal_year の判定
    # docDescriptionから「YYYY年M月期」パターンを抽出（TDnet方式と同じ）
    year_match = re.search(r'(\d{4})年.*?期', normalized_desc)
    if year_match:
        fiscal_year = year_match.group(1)
    else:
        # フォールバック: periodStartから決算年度を算出
        # periodStart月==1 → 12月決算（同年）、それ以外 → 翌年に決算期末
        period_start = doc_info.get('periodStart', '')
        if period_start and len(period_start) >= 7:
            start_year = int(period_start[:4])
            start_month = int(period_start[5:7])
            fiscal_year = str(start_year) if start_month == 1 else str(start_year + 1)
        elif period_end:
            fiscal_year = period_end[:4]
        else:
            submit_date = doc_info.get('submitDateTime', '')[:10]
            fiscal_year = submit_date[:4]

    return fiscal_year, fiscal_quarter


def process_document(client: EdinetClient, doc_info: dict,
                     edinet_map: dict = None, processed_ids: set = None) -> bool:
    """
    1つの書類を処理

    Args:
        client: EdinetClientインスタンス
        doc_info: 書類情報（APIレスポンスの1要素）
        edinet_map: EDINETコード→証券コードのマッピング（キャッシュ）
        processed_ids: 処理済み書類IDの集合
    """
    doc_id = doc_info.get('docID')
    edinet_code = doc_info.get('edinetCode')
    filer_name = doc_info.get('filerName', '')
    doc_type = doc_info.get('docTypeCode')
    period_end = doc_info.get('periodEnd')
    submit_date = doc_info.get('submitDateTime', '')[:10]

    # 処理済みならスキップ（DL不要）
    if processed_ids and doc_id and doc_id in processed_ids:
        print(f"  [SKIP] 処理済み: {doc_id} ({filer_name})")
        return False

    # 証券コードを取得（キャッシュ優先）
    if edinet_map is not None:
        ticker_code = edinet_map.get(edinet_code)
    else:
        ticker_code = edinet_code_to_ticker(edinet_code)

    if not ticker_code:
        print(f"  [SKIP] EDINETコード未登録: {edinet_code} ({filer_name})")
        return False

    print(f"  処理中: {ticker_code} - {filer_name} ({DOC_TYPE_CODES.get(doc_type, doc_type)})")

    # 書類をダウンロード
    zip_content = client.download_document(doc_id)
    if not zip_content:
        return False

    # ZIPを展開
    extracted_paths = extract_edinet_zip(zip_content)
    if not extracted_paths:
        return False

    # temp_dirを特定（クリーンアップ用）
    # extracted_pathsはリスト、最初の要素からtemp_dirを特定
    first_path = extracted_paths[0]
    temp_dir = first_path
    while temp_dir.parent != temp_dir and not str(temp_dir.name).startswith("edinet_"):
        temp_dir = temp_dir.parent
    # edinet_プレフィックスが見つからない場合はfirst_pathの最上位を使う
    if not str(temp_dir.name).startswith("edinet_"):
        temp_dir = first_path
        while temp_dir.parent != temp_dir and temp_dir.parent != Path(tempfile.gettempdir()):
            temp_dir = temp_dir.parent

    try:
        # パース方法を判定
        first_file = extracted_paths[0]
        if first_file.name.lower().startswith('manifest') and first_file.suffix == '.xml':
            # iXBRL形式 → XBRLPパーサー使用（manifestは単一Pathで渡してprepare_ixbrlを呼ぶ）
            print(f"    パーサー: XBRLP (iXBRL)")
            financials = parse_ixbrl_financials(first_file)
        elif first_file.suffix.lower() in ['.htm', '.html']:
            # 直接htmファイル → XBRLPパーサー使用
            print(f"    パーサー: XBRLP (iXBRL)")
            financials = parse_ixbrl_financials(extracted_paths)
        else:
            # 旧形式 .xbrl → レガシーパーサー使用
            print(f"    パーサー: レガシー (.xbrl)")
            xbrl_content = first_file.read_text(encoding='utf-8')
            financials = _parse_xbrl_legacy(xbrl_content)

        if not financials:
            print(f"    [WARN] 財務データを抽出できませんでした")
            return False

        # 決算期を判定
        fiscal_year, fiscal_quarter = _detect_edinet_quarter(doc_info)

        # DBに保存
        insert_financial(
            ticker_code=ticker_code,
            fiscal_year=fiscal_year,
            fiscal_quarter=fiscal_quarter,
            fiscal_end_date=period_end,
            announcement_date=submit_date,
            revenue=financials.get('revenue'),
            gross_profit=financials.get('gross_profit'),
            operating_income=financials.get('operating_income'),
            ordinary_income=financials.get('ordinary_income'),
            net_income=financials.get('net_income'),
            eps=financials.get('eps'),
            source='EDINET',
            edinet_doc_id=doc_id
        )

        fields = {
            '売上': financials.get('revenue'),
            '売上総利益': financials.get('gross_profit'),
            '営業利益': financials.get('operating_income'),
            '経常利益': financials.get('ordinary_income'),
            '純利益': financials.get('net_income'),
            'EPS': financials.get('eps'),
        }
        missing = [k for k, v in fields.items() if v is None]
        detail = ", ".join(f"{k}={v}" for k, v in fields.items())
        if missing:
            print(f"    [一部欠損] [{ticker_code} {period_end}] {detail}（欠損: {', '.join(missing)}）")
        else:
            print(f"    保存完了: [{ticker_code} {period_end}] {detail}")
        return True

    finally:
        # 一時ディレクトリを確実にクリーンアップ
        shutil.rmtree(temp_dir, ignore_errors=True)


def fetch_financials(days: int = 7, tickers: list = None, api_key: str = None,
                     force: bool = False):
    """
    決算データを取得

    Args:
        days: 過去何日分を取得するか
        tickers: 対象銘柄リスト（Noneなら全銘柄）
        api_key: EDINET APIキー
        force: Trueなら処理済み書類も再取得
    """
    log_id = log_batch_start("fetch_financials")
    processed = 0

    client = EdinetClient(api_key=api_key)

    # マッピングを一括ロード（DB接続を最小化）
    edinet_map = get_edinet_ticker_map()
    processed_ids = None if force else get_processed_doc_ids()
    skip_info = "無効(--force)" if force else f"{len(processed_ids)}件"
    print(f"決算データ取得開始: 過去{days}日分（追跡銘柄: {len(edinet_map)}社, 処理済みスキップ: {skip_info}）")
    print("-" * 50)

    try:
        # 日付ごとに処理
        for i in range(days):
            target_date = (datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d')
            print(f"\n[{target_date}]")

            # 書類一覧を取得（有報・半期報・四半期報）
            all_docs = client.get_document_list(target_date)
            target_types = {'120', '140', '160'}
            docs = [d for d in all_docs if d.get('docTypeCode') in target_types]

            if not docs:
                print("  書類なし")
                continue

            print(f"  {len(docs)}件の書類")

            for doc in docs:
                # 対象銘柄フィルタ（キャッシュ参照、DB不要）
                if tickers:
                    edinet_code = doc.get('edinetCode')
                    ticker = edinet_map.get(edinet_code)
                    if ticker not in tickers:
                        continue

                result = process_document(client, doc, edinet_map, processed_ids)
                if result:
                    processed += 1
                    # API制限対策（ダウンロード実行時のみ）
                    time.sleep(1.0)

        log_batch_end(log_id, "success", processed)
        print("-" * 50)
        print(f"完了: {processed}件の決算データを処理")

    except Exception as e:
        log_batch_end(log_id, "failed", processed, str(e))
        print(f"\n[ERROR] バッチ失敗: {e}")
        raise


def _load_env():
    """プロジェクトルートの.envファイルから環境変数を読み込む"""
    env_path = BASE_DIR / ".env"
    if not env_path.exists():
        return
    with open(env_path, encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if '=' in line:
                key, _, value = line.partition('=')
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                os.environ.setdefault(key, value)


def main():
    _load_env()

    parser = argparse.ArgumentParser(description='EDINETから決算データを取得')
    parser.add_argument('--days', type=int, default=7, help='過去N日分を取得')
    parser.add_argument('--ticker', '-t', help='特定銘柄のみ取得（カンマ区切り）')
    parser.add_argument('--api-key', help='EDINET APIキー（未指定時は環境変数 EDINET_API_KEY）')
    parser.add_argument('--doc-id', help='特定の書類IDを処理')
    parser.add_argument('--force', action='store_true', help='処理済み書類も再取得')
    args = parser.parse_args()

    # APIキー: 引数 > 環境変数
    api_key = args.api_key or os.environ.get('EDINET_API_KEY')

    # 対象銘柄
    tickers = None
    if args.ticker:
        tickers = [t.strip() for t in args.ticker.split(',')]

    if args.doc_id:
        client = EdinetClient(api_key=api_key)
        doc_info = {'docID': args.doc_id}
        process_document(client, doc_info)
    else:
        fetch_financials(days=args.days, tickers=tickers, api_key=api_key,
                        force=args.force)


if __name__ == "__main__":
    main()
