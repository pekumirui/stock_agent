"""
銘柄マスタ初期化スクリプト

日本取引所グループ（JPX）の銘柄リストを取得して
companiesテーブルに登録する

使用方法:
    python init_companies.py
"""
import requests
import pandas as pd
import io
from db_utils import get_connection, init_database, upsert_company, is_valid_ticker_code


# JPXの上場銘柄一覧CSVのURL
# 参考: https://www.jpx.co.jp/markets/statistics-equities/misc/01.html
JPX_LIST_URL = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"


def fetch_jpx_companies_from_web():
    """JPXから上場銘柄一覧を取得"""
    print("JPXから銘柄一覧を取得中...")
    
    try:
        # xlsファイルを取得
        response = requests.get(JPX_LIST_URL, timeout=30)
        response.raise_for_status()
        
        # pandasで読み込み
        df = pd.read_excel(io.BytesIO(response.content))
        
        print(f"取得完了: {len(df)}銘柄")
        return df
        
    except Exception as e:
        print(f"JPXからの取得に失敗: {e}")
        return None


def parse_jpx_data(df: pd.DataFrame) -> list:
    """JPXデータをパースして銘柄情報のリストに変換"""
    companies = []
    
    # カラム名を確認（日本語カラム名の場合がある）
    # 典型的なカラム: コード, 銘柄名, 市場・商品区分, 33業種コード, 33業種区分, 17業種コード, 17業種区分
    
    for _, row in df.iterrows():
        try:
            # コード列を探す
            ticker_code = None
            company_name = None
            market_segment = None
            sector_33 = None
            sector_17 = None
            
            for col in df.columns:
                col_str = str(col)
                col_lower = col_str.lower()
                value = row[col]

                # 「コード」完全一致（「33業種コード」等を除外）
                if col_str == 'コード' or col_lower == 'code':
                    ticker_code = str(value).strip()
                elif '銘柄名' in col_str or '銘柄' in col_str or 'name' in col_lower:
                    company_name = str(value).strip() if pd.notna(value) else None
                elif '市場' in col_str or 'market' in col_lower:
                    market_segment = str(value).strip() if pd.notna(value) else None
                elif '33業種区分' in col_str:
                    sector_33 = str(value).strip() if pd.notna(value) else None
                elif '17業種区分' in col_str:
                    sector_17 = str(value).strip() if pd.notna(value) else None
            
            # 有効なデータのみ追加
            if ticker_code and company_name and is_valid_ticker_code(ticker_code):
                companies.append({
                    'ticker_code': ticker_code,
                    'company_name': company_name,
                    'market_segment': market_segment,
                    'sector_33': sector_33,
                    'sector_17': sector_17
                })
                
        except Exception as e:
            continue
    
    return companies


def init_companies_from_csv(csv_path: str):
    """CSVファイルから銘柄マスタを初期化"""
    print(f"CSVから銘柄を読み込み: {csv_path}")
    
    df = pd.read_csv(csv_path, dtype={'ticker_code': str, 'コード': str, 'code': str})
    companies = parse_jpx_data(df)
    
    return companies


def init_companies_from_sample():
    """サンプル銘柄（主要銘柄）を登録"""
    print("サンプル銘柄を登録...")
    
    # 日経225の主要銘柄（一部）
    sample_companies = [
        # 自動車
        ('7203', 'トヨタ自動車', 'プライム', '輸送用機器'),
        ('7267', '本田技研工業', 'プライム', '輸送用機器'),
        ('7201', '日産自動車', 'プライム', '輸送用機器'),
        # 電機
        ('6758', 'ソニーグループ', 'プライム', '電気機器'),
        ('6861', 'キーエンス', 'プライム', '電気機器'),
        ('6902', 'デンソー', 'プライム', '電気機器'),
        ('6501', '日立製作所', 'プライム', '電気機器'),
        ('6594', '日本電産', 'プライム', '電気機器'),
        # 通信・IT
        ('9432', '日本電信電話', 'プライム', '情報・通信業'),
        ('9433', 'KDDI', 'プライム', '情報・通信業'),
        ('9434', 'ソフトバンク', 'プライム', '情報・通信業'),
        ('9984', 'ソフトバンクグループ', 'プライム', '情報・通信業'),
        # 金融
        ('8306', '三菱UFJフィナンシャル・グループ', 'プライム', '銀行業'),
        ('8316', '三井住友フィナンシャルグループ', 'プライム', '銀行業'),
        ('8411', 'みずほフィナンシャルグループ', 'プライム', '銀行業'),
        # 商社
        ('8058', '三菱商事', 'プライム', '卸売業'),
        ('8031', '三井物産', 'プライム', '卸売業'),
        ('8001', '伊藤忠商事', 'プライム', '卸売業'),
        # 小売
        ('9983', 'ファーストリテイリング', 'プライム', '小売業'),
        ('7974', '任天堂', 'プライム', 'その他製品'),
        # 医薬品
        ('4502', '武田薬品工業', 'プライム', '医薬品'),
        ('4503', 'アステラス製薬', 'プライム', '医薬品'),
        ('4568', '第一三共', 'プライム', '医薬品'),
        # 化学
        ('4063', '信越化学工業', 'プライム', '化学'),
        # 食品
        ('2914', '日本たばこ産業', 'プライム', '食料品'),
        # 建設・不動産
        ('1925', '大和ハウス工業', 'プライム', '建設業'),
        # 半導体
        ('8035', '東京エレクトロン', 'プライム', '電気機器'),
        ('6857', 'アドバンテスト', 'プライム', '電気機器'),
        ('6920', 'レーザーテック', 'プライム', '電気機器'),
        # その他注目銘柄
        ('4661', 'オリエンタルランド', 'プライム', 'サービス業'),
        ('6098', 'リクルートホールディングス', 'プライム', 'サービス業'),
    ]
    
    return [
        {
            'ticker_code': t[0],
            'company_name': t[1],
            'market_segment': t[2],
            'sector_33': t[3]
        }
        for t in sample_companies
    ]


def register_companies(companies: list):
    """銘柄リストをDBに登録"""
    print(f"DBに{len(companies)}銘柄を登録中...")
    
    registered = 0
    for company in companies:
        try:
            upsert_company(
                ticker_code=company['ticker_code'],
                company_name=company['company_name'],
                market_segment=company.get('market_segment'),
                sector_33=company.get('sector_33'),
                sector_17=company.get('sector_17')
            )
            registered += 1
        except Exception as e:
            print(f"  [ERROR] {company['ticker_code']}: {e}")
    
    print(f"登録完了: {registered}銘柄")
    return registered


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='銘柄マスタを初期化')
    parser.add_argument('--csv', help='CSVファイルから読み込み')
    parser.add_argument('--sample', action='store_true', help='サンプル銘柄のみ登録（テスト用）')
    parser.add_argument('--jpx', action='store_true', help='JPXから全銘柄を取得（デフォルト）')
    args = parser.parse_args()
    
    # DB初期化
    init_database()
    
    # 銘柄データを取得
    if args.csv:
        companies = init_companies_from_csv(args.csv)
    elif args.sample:
        companies = init_companies_from_sample()
    else:
        # JPXから取得を試みる
        df = fetch_jpx_companies_from_web()
        if df is not None:
            companies = parse_jpx_data(df)
        else:
            print("JPXからの取得に失敗したため、サンプル銘柄を使用します")
            companies = init_companies_from_sample()
    
    if not companies:
        print("銘柄データがありません")
        return
    
    # DBに登録
    register_companies(companies)
    
    # 確認
    with get_connection() as conn:
        cursor = conn.execute("SELECT COUNT(*) FROM companies")
        count = cursor.fetchone()[0]
        print(f"\n銘柄マスタ総数: {count}銘柄")


if __name__ == "__main__":
    main()
