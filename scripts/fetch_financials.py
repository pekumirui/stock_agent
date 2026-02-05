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
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Optional, Dict, Any
import xml.etree.ElementTree as ET

# XBRLPライブラリのインポート
BASE_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(BASE_DIR / "lib"))
from xbrlp import Parser, Fact, QName, FileLoader

from db_utils import (
    get_connection, get_all_tickers, insert_financial,
    log_batch_start, log_batch_end
)


# EDINET API エンドポイント
EDINET_API_BASE = "https://api.edinet-fsa.go.jp/api/v2"

# 書類種別コード
DOC_TYPE_CODES = {
    '120': '有価証券報告書',
    '130': '四半期報告書',
    '140': '半期報告書',
    '150': '臨時報告書',
    '160': '自己株券買付状況報告書',
    '170': '発行登録追補書類',
}

# XBRLP用: QName local_name → DBフィールドのマッピング
XBRL_FACT_MAPPING = {
    # 売上高
    'NetSales': 'revenue',
    'Revenue': 'revenue',
    'OperatingRevenue': 'revenue',
    # 売上総利益
    'GrossProfit': 'gross_profit',
    # 営業利益
    'OperatingIncome': 'operating_income',
    'OperatingProfit': 'operating_income',
    # 経常利益
    'OrdinaryIncome': 'ordinary_income',
    'OrdinaryProfit': 'ordinary_income',
    # 当期純利益
    'ProfitLoss': 'net_income',
    'NetIncome': 'net_income',
    'ProfitLossAttributableToOwnersOfParent': 'net_income',
    # EPS
    'BasicEarningsLossPerShare': 'eps',
    'EarningsPerShare': 'eps',
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
    ],
}

# jppfs名前空間パターン
JPPFS_NAMESPACE_PATTERNS = [
    'jppfs_cor',
    'http://disclosure.edinet-fsa.go.jp/taxonomy/jppfs',
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


def extract_edinet_zip(zip_content: bytes) -> Optional[Path]:
    """
    EDINETのZIPを一時ディレクトリに展開し、
    manifestファイルまたはXBRLファイルのパスを返す

    EDINET ZIP構造:
      XBRL/
        PublicDoc/
          manifest.xml (or manifest_*.xml)
          *.htm / *.html (iXBRLファイル)
          *.xbrl (旧形式)
          *.xsd
          *_lab.xml, *_pre.xml, *_cal.xml (linkbase)

    TDnet ZIP構造:
      summary/
        XBRL/PublicDoc/
          *.htm / *.html (iXBRLファイル)
          *.xbrl
          manifest*.xml

    Returns:
        展開されたファイルのパス（manifestまたは.xbrl）。
        呼び出し側でtemp_dirの削除が必要。
    """
    temp_dir = Path(tempfile.mkdtemp(prefix="edinet_"))
    try:
        with zipfile.ZipFile(io.BytesIO(zip_content)) as zf:
            zf.extractall(temp_dir)

        # デバッグ: ZIP内のファイル構造を出力（最初の10ファイル）
        all_files = list(temp_dir.rglob("*"))[:10]
        print(f"    [DEBUG] ZIP内のファイル（最初10件）: {[str(f.relative_to(temp_dir)) for f in all_files if f.is_file()]}")

        # manifestファイルを探す（iXBRL形式）
        for manifest in sorted(temp_dir.rglob("manifest*.xml")):
            print(f"    [DEBUG] manifestファイル発見: {manifest.relative_to(temp_dir)}")
            # PublicDoc制約を緩和（TDnet対応）
            if "PublicDoc" in str(manifest) or "XBRL" in str(manifest):
                return manifest

        # manifestがない場合は.xbrlファイルを探す（旧形式フォールバック）
        for xbrl_file in sorted(temp_dir.rglob("*.xbrl")):
            print(f"    [DEBUG] XBRLファイル発見: {xbrl_file.relative_to(temp_dir)}")
            # PublicDoc制約を緩和（TDnet対応）
            if "PublicDoc" in str(xbrl_file) or "XBRL" in str(xbrl_file):
                return xbrl_file

        print(f"    [WARN] ZIPにXBRL関連ファイルが見つかりません")
        shutil.rmtree(temp_dir, ignore_errors=True)
        return None
    except Exception as e:
        print(f"  [ERROR] ZIP展開失敗: {e}")
        shutil.rmtree(temp_dir, ignore_errors=True)
        return None


def _is_jppfs_namespace(qname: QName) -> bool:
    """QNameがjppfs_cor名前空間に属するか判定"""
    if qname.prefix and 'jppfs' in qname.prefix:
        return True
    if qname.namespace_uri:
        for pattern in JPPFS_NAMESPACE_PATTERNS:
            if pattern in qname.namespace_uri:
                return True
    return False


def _is_current_period_context(context_ref: str) -> bool:
    """当期のコンテキストか判定"""
    ctx = context_ref.lower()
    if 'prior' in ctx:
        return False
    if 'current' in ctx:
        return True
    return False


def parse_ixbrl_financials(manifest_path: Path) -> Dict[str, Any]:
    """
    XBRLPを使ってiXBRLから財務データを抽出

    Args:
        manifest_path: EDINET ZIP内のmanifest.xmlパス

    Returns:
        {'revenue': 123456.0, 'operating_income': 12345.0, ...}
    """
    parser = Parser(file_loader=_file_loader)

    try:
        parser.prepare_ixbrl(manifest_path)
    except ValueError:
        # manifestからiXBRLファイルが見つからない場合、
        # PublicDoc内の.htm/.htmlファイルを直接探す
        public_doc_dir = manifest_path.parent
        htm_files = list(public_doc_dir.glob("*.htm")) + list(public_doc_dir.glob("*.html"))
        if htm_files:
            parser.ixbrl_files = htm_files
        else:
            print(f"    [WARN] iXBRLファイルが見つかりません")
            return {}

    financials = {}

    try:
        for fact in parser.load_facts():
            # jppfs_cor名前空間のFactのみ対象
            if not _is_jppfs_namespace(fact.qname):
                continue

            # 当期データのみ
            if not _is_current_period_context(fact.context_ref):
                continue

            # マッピング対象か確認
            db_field = XBRL_FACT_MAPPING.get(fact.qname.local_name)
            if not db_field:
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


def process_document(client: EdinetClient, doc_info: dict) -> bool:
    """
    1つの書類を処理

    Args:
        client: EdinetClientインスタンス
        doc_info: 書類情報（APIレスポンスの1要素）
    """
    doc_id = doc_info.get('docID')
    edinet_code = doc_info.get('edinetCode')
    filer_name = doc_info.get('filerName', '')
    doc_type = doc_info.get('docTypeCode')
    period_end = doc_info.get('periodEnd')
    submit_date = doc_info.get('submitDateTime', '')[:10]

    # 証券コードを取得
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
    extracted_path = extract_edinet_zip(zip_content)
    if not extracted_path:
        return False

    # temp_dirを特定（クリーンアップ用）
    # extracted_pathは temp_dir/XBRL/PublicDoc/manifest.xml のようなパス
    temp_dir = extracted_path
    while temp_dir.parent != temp_dir and not str(temp_dir.name).startswith("edinet_"):
        temp_dir = temp_dir.parent
    # edinet_プレフィックスが見つからない場合はextracted_pathの最上位を使う
    if not str(temp_dir.name).startswith("edinet_"):
        temp_dir = extracted_path
        while temp_dir.parent != temp_dir and temp_dir.parent != Path(tempfile.gettempdir()):
            temp_dir = temp_dir.parent

    try:
        # パース方法を判定
        if extracted_path.name.lower().startswith('manifest') and extracted_path.suffix == '.xml':
            # iXBRL形式 → XBRLPパーサー使用
            print(f"    パーサー: XBRLP (iXBRL)")
            financials = parse_ixbrl_financials(extracted_path)
        else:
            # 旧形式 .xbrl → レガシーパーサー使用
            print(f"    パーサー: レガシー (.xbrl)")
            xbrl_content = extracted_path.read_text(encoding='utf-8')
            financials = _parse_xbrl_legacy(xbrl_content)

        if not financials:
            print(f"    [WARN] 財務データを抽出できませんでした")
            return False

        # 決算期を判定
        fiscal_year = period_end[:4] if period_end else submit_date[:4]
        fiscal_quarter = 'FY'  # 有報は通期
        if doc_type == '130':  # 四半期報告書
            if period_end:
                month = int(period_end[5:7])
                if month in [3, 6]:
                    fiscal_quarter = 'Q1'
                elif month in [6, 9]:
                    fiscal_quarter = 'Q2'
                elif month in [9, 12]:
                    fiscal_quarter = 'Q3'

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

        print(f"    保存完了: 売上={financials.get('revenue')}, 営業利益={financials.get('operating_income')}")
        return True

    finally:
        # 一時ディレクトリを確実にクリーンアップ
        shutil.rmtree(temp_dir, ignore_errors=True)


def fetch_financials(days: int = 7, tickers: list = None, api_key: str = None):
    """
    決算データを取得

    Args:
        days: 過去何日分を取得するか
        tickers: 対象銘柄リスト（Noneなら全銘柄）
        api_key: EDINET APIキー
    """
    log_id = log_batch_start("fetch_financials")
    processed = 0

    client = EdinetClient(api_key=api_key)

    print(f"決算データ取得開始: 過去{days}日分")
    print("-" * 50)

    try:
        # 日付ごとに処理
        for i in range(days):
            target_date = (datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d')
            print(f"\n[{target_date}]")

            # 有価証券報告書を取得
            docs = client.get_document_list(target_date, doc_type='120')
            docs += client.get_document_list(target_date, doc_type='130')  # 四半期報告書

            if not docs:
                print("  書類なし")
                continue

            print(f"  {len(docs)}件の書類")

            for doc in docs:
                # 対象銘柄フィルタ
                if tickers:
                    edinet_code = doc.get('edinetCode')
                    ticker = edinet_code_to_ticker(edinet_code)
                    if ticker not in tickers:
                        continue

                if process_document(client, doc):
                    processed += 1

                # API制限対策
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
        fetch_financials(days=args.days, tickers=tickers, api_key=api_key)


if __name__ == "__main__":
    main()
