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
import requests
import zipfile
import io
import time
import re
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, Any
import xml.etree.ElementTree as ET

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

# 財務項目のXBRLタグ（日本基準）
XBRL_TAGS = {
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
    
    def extract_xbrl_from_zip(self, zip_content: bytes) -> Optional[str]:
        """ZIPからXBRLファイルを抽出"""
        try:
            with zipfile.ZipFile(io.BytesIO(zip_content)) as zf:
                for name in zf.namelist():
                    # メインのXBRLファイルを探す（通常 XBRL/PublicDoc/ 配下）
                    if name.endswith('.xbrl') and 'PublicDoc' in name:
                        return zf.read(name).decode('utf-8')
            return None
        except Exception as e:
            print(f"  [ERROR] XBRL抽出失敗: {e}")
            return None


def parse_xbrl_financials(xbrl_content: str) -> Dict[str, Any]:
    """
    XBRLから財務データを抽出
    
    Returns:
        {'revenue': 123456, 'operating_income': 12345, ...}
    """
    financials = {}
    
    try:
        # XMLパース
        root = ET.fromstring(xbrl_content)
        
        # 名前空間を取得
        namespaces = dict([node for _, node in ET.iterparse(io.StringIO(xbrl_content), events=['start-ns'])])
        
        # 各財務項目を検索
        for field, tags in XBRL_TAGS.items():
            for tag in tags:
                # タグ名からプレフィックスと要素名を分離
                prefix, element = tag.split(':')
                
                # 名前空間付きで検索
                for ns_prefix, ns_uri in namespaces.items():
                    if prefix in ns_prefix or prefix.lower() in ns_uri.lower():
                        xpath = f".//{{{ns_uri}}}{element}"
                        elements = root.findall(xpath)
                        
                        for elem in elements:
                            # contextRefで当期のデータを特定
                            context = elem.get('contextRef', '')
                            if 'Current' in context or 'current' in context:
                                try:
                                    value = float(elem.text)
                                    # 単位変換（円→百万円）
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
        print(f"  [ERROR] XBRLパース失敗: {e}")
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
    
    # 証券コードを取得（EDINETコードからの変換、または会社名から推測）
    ticker_code = edinet_code_to_ticker(edinet_code)
    
    if not ticker_code:
        # EDINETコード未登録の場合はスキップ（または登録）
        print(f"  [SKIP] EDINETコード未登録: {edinet_code} ({filer_name})")
        return False
    
    print(f"  処理中: {ticker_code} - {filer_name} ({DOC_TYPE_CODES.get(doc_type, doc_type)})")
    
    # 書類をダウンロード
    zip_content = client.download_document(doc_id)
    if not zip_content:
        return False
    
    # XBRLを抽出
    xbrl_content = client.extract_xbrl_from_zip(zip_content)
    if not xbrl_content:
        print(f"    [WARN] XBRLファイルが見つかりません")
        return False
    
    # 財務データを抽出
    financials = parse_xbrl_financials(xbrl_content)
    
    if not financials:
        print(f"    [WARN] 財務データを抽出できませんでした")
        return False
    
    # 決算期を判定
    fiscal_year = period_end[:4] if period_end else submit_date[:4]
    fiscal_quarter = 'FY'  # 有報は通期
    if doc_type == '130':  # 四半期報告書
        # 期末日から四半期を判定
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


def main():
    parser = argparse.ArgumentParser(description='EDINETから決算データを取得')
    parser.add_argument('--days', type=int, default=7, help='過去N日分を取得')
    parser.add_argument('--ticker', '-t', help='特定銘柄のみ取得（カンマ区切り）')
    parser.add_argument('--api-key', help='EDINET APIキー')
    parser.add_argument('--doc-id', help='特定の書類IDを処理')
    args = parser.parse_args()
    
    # 対象銘柄
    tickers = None
    if args.ticker:
        tickers = [t.strip() for t in args.ticker.split(',')]
    
    if args.doc_id:
        # 特定書類のみ処理
        client = EdinetClient(api_key=args.api_key)
        doc_info = {'docID': args.doc_id}  # 最小限の情報
        process_document(client, doc_info)
    else:
        fetch_financials(days=args.days, tickers=tickers, api_key=args.api_key)


if __name__ == "__main__":
    main()
