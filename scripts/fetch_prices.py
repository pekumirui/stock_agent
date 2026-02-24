"""
株価取得バッチ - Yahoo Finance
日次で株価データを取得してDBに保存

使用方法:
    python fetch_prices.py                    # 全銘柄の最新データを取得
    python fetch_prices.py --ticker 7203      # 特定銘柄のみ取得
    python fetch_prices.py --full             # 全期間の履歴を取得（初回用）
    python fetch_prices.py --days 30          # 過去30日分を取得
    python fetch_prices.py --batch-size 50    # 一括取得する銘柄数を指定
    
    # 目安:
    #   --batch-size は大きいほど高速だが、API制限に近づく場合は小さくする
    #   --sleep はバッチ単位の待機（秒）。制限に当たるなら少し増やす
"""
import argparse
import time
from datetime import datetime, timedelta
from typing import Optional
import yfinance as yf
import pandas as pd

from db_utils import (
    get_connection, get_all_tickers, get_last_price_date,
    bulk_insert_prices, insert_stock_split, upsert_company,
    log_batch_start, log_batch_end
)


def ticker_to_yahoo_symbol(ticker_code: str) -> str:
    """証券コードをYahoo Finance用シンボルに変換"""
    # 日本株は末尾に.Tを付ける
    return f"{ticker_code}.T"


def fetch_stock_data_batch(ticker_codes: list, start_date: str = None, end_date: str = None,
                           period: str = None) -> pd.DataFrame:
    """
    Yahoo Financeから複数銘柄の株価データを一括取得

    Args:
        ticker_codes: 証券コード（4桁）のリスト
        start_date: 取得開始日（YYYY-MM-DD）
        end_date: 取得終了日（YYYY-MM-DD）
        period: 期間指定（例: "1y", "6mo", "max"）startより優先

    Returns:
        株価DataFrame（複数銘柄はMultiIndex列）
    """
    symbols = [ticker_to_yahoo_symbol(t) for t in ticker_codes]

    params = {
        "tickers": " ".join(symbols),
        "auto_adjust": False,
        "actions": True,
        "group_by": "ticker",
        "threads": False,
        "progress": False,
    }
    if period:
        params["period"] = period
    elif start_date:
        params["start"] = start_date
        if end_date:
            params["end"] = end_date
    else:
        params["period"] = "5d"

    try:
        df = yf.download(**params)
        return df
    except Exception as e:
        print(f"  [ERROR] バッチ取得失敗: {e}")
        return pd.DataFrame()


def process_price_data(ticker_code: str, df: pd.DataFrame) -> list:
    """DataFrameをDB挿入用のタプルリストに変換"""
    if df.empty:
        return []
    
    records = []
    for date, row in df.iterrows():
        # dateがTimestampの場合は文字列に変換
        trade_date = date.strftime('%Y-%m-%d') if hasattr(date, 'strftime') else str(date)[:10]
        
        records.append((
            ticker_code,
            trade_date,
            float(row['Open']) if pd.notna(row['Open']) else None,
            float(row['High']) if pd.notna(row['High']) else None,
            float(row['Low']) if pd.notna(row['Low']) else None,
            float(row['Close']) if pd.notna(row['Close']) else None,
            int(row['Volume']) if pd.notna(row['Volume']) else 0,
            float(row['Adj Close']) if pd.notna(row.get('Adj Close', row['Close'])) else None
        ))
    
    return records


def fetch_all_prices(tickers: list, start_date: str = None, period: str = None,
                     sleep_interval: float = 0.5, batch_size: int = 50):
    """
    複数銘柄の株価を一括取得
    
    Args:
        tickers: 証券コードのリスト
        start_date: 取得開始日
        period: 期間指定
        sleep_interval: API呼び出し間隔（秒）
    """
    log_id = log_batch_start("fetch_prices")
    total_records = 0
    errors = []
    
    print(f"株価取得開始: {len(tickers)}銘柄")
    print(f"期間: {period or start_date or '直近5日'}")
    print("-" * 50)
    
    def _iter_batches(items: list, size: int):
        for i in range(0, len(items), size):
            yield items[i:i + size]

    try:
        for batch_index, batch in enumerate(_iter_batches(tickers, batch_size), 1):
            print(f"[BATCH {batch_index}] {len(batch)}銘柄取得中...")

            df = fetch_stock_data_batch(batch, start_date=start_date, period=period)
            if df.empty:
                print("  バッチ結果なし")
                time.sleep(sleep_interval)
                continue

            for i, ticker_code in enumerate(batch, 1):
                symbol = ticker_to_yahoo_symbol(ticker_code)
                print(f"  [{i}/{len(batch)}] {ticker_code}...", end=" ")

                if isinstance(df.columns, pd.MultiIndex):
                    if symbol not in df.columns.levels[0]:
                        print("データなし")
                        continue
                    data = df[symbol]
                else:
                    data = df

                if data.empty:
                    print("データなし")
                    continue

                # DB挿入用に変換
                records = process_price_data(ticker_code, data)

                if records:
                    inserted = bulk_insert_prices(records)
                    total_records += inserted
                    print(f"{len(records)}件取得, {inserted}件挿入")
                else:
                    print("レコードなし")

                # 分割情報を保存（actions=Trueで取得）
                if "Stock Splits" in data.columns:
                    splits_series = data["Stock Splits"].dropna()
                    splits_series = splits_series[splits_series != 0]
                    for date, ratio in splits_series.items():
                        split_date = date.strftime('%Y-%m-%d') if hasattr(date, 'strftime') else str(date)[:10]
                        insert_stock_split(
                            ticker_code,
                            split_date,
                            1.0,
                            float(ratio)
                        )

            # API制限対策（バッチ単位で待機）
            time.sleep(sleep_interval)
        
        log_batch_end(log_id, "success", total_records)
        print("-" * 50)
        print(f"完了: {total_records}件のレコードを挿入")
        
    except Exception as e:
        log_batch_end(log_id, "failed", total_records, str(e))
        print(f"\n[ERROR] バッチ失敗: {e}")
        raise


def fetch_company_info(ticker_code: str) -> dict:
    """銘柄の企業情報を取得"""
    symbol = ticker_to_yahoo_symbol(ticker_code)
    
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info
        
        return {
            'ticker_code': ticker_code,
            'company_name': info.get('longName') or info.get('shortName', ''),
            'company_name_en': info.get('shortName', ''),
            'sector_33': info.get('sector', ''),
            'market_segment': info.get('market', '')
        }
    except Exception as e:
        print(f"  [ERROR] 企業情報取得失敗 {ticker_code}: {e}")
        return None


def main():
    parser = argparse.ArgumentParser(description='Yahoo Financeから株価を取得')
    parser.add_argument('--ticker', '-t', help='特定銘柄のみ取得（カンマ区切りで複数指定可）')
    parser.add_argument('--full', action='store_true', help='全期間の履歴を取得')
    parser.add_argument('--days', type=int, help='過去N日分を取得')
    parser.add_argument('--period', help='期間指定（例: 1y, 6mo, max）')
    parser.add_argument('--sleep', type=float, default=0.3, help='API呼び出し間隔（秒）')
    parser.add_argument('--batch-size', type=int, default=50, help='一括取得する銘柄数')
    args = parser.parse_args()
    
    # 対象銘柄を決定
    if args.ticker:
        tickers = [t.strip() for t in args.ticker.split(',')]
    else:
        tickers = get_all_tickers()
        if not tickers:
            print("銘柄マスタが空です。先に銘柄を登録してください。")
            return
    
    # 期間を決定
    if args.full:
        period = "max"
        start_date = None
    elif args.period:
        period = args.period
        start_date = None
    elif args.days:
        period = None
        start_date = (datetime.now() - timedelta(days=args.days)).strftime('%Y-%m-%d')
    else:
        # デフォルト: 最後の取得日から
        last_date = get_last_price_date()
        if last_date:
            start_date = last_date
            period = None
        else:
            period = "5d"
            start_date = None
    
    fetch_all_prices(
        tickers,
        start_date=start_date,
        period=period,
        sleep_interval=args.sleep,
        batch_size=args.batch_size
    )


if __name__ == "__main__":
    main()
