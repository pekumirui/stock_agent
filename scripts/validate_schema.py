"""
株価データスキーマ検証スクリプト
yfinanceから2年分の株価データを取得し、スキーマ適合性を検証

使用方法:
    python validate_schema.py                    # 全銘柄を検証
    python validate_schema.py --ticker 7203      # 特定銘柄のみ
    python validate_schema.py --ticker 7203,9984 # 複数銘柄
    python validate_schema.py --dry-run          # DB挿入せずCSV保存のみ
"""
import argparse
import time
import sqlite3
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass, field
from typing import Optional

import yfinance as yf
import pandas as pd

from db_utils import get_connection, get_all_tickers, init_database

# パス設定
BASE_DIR = Path(__file__).parent.parent
CSV_DIR = BASE_DIR / "data" / "csv"
TEST_DB_PATH = BASE_DIR / "db" / "test_validation.db"
SCHEMA_PATH = BASE_DIR / "db" / "schema.sql"


@dataclass
class ValidationResult:
    """検証結果を保持"""
    ticker_code: str
    success: bool = True
    records_count: int = 0
    csv_saved: bool = False
    db_inserted: bool = False
    errors: list = field(default_factory=list)
    warnings: list = field(default_factory=list)


def ticker_to_yahoo_symbol(ticker_code: str) -> str:
    """証券コードをYahoo Finance用シンボルに変換"""
    return f"{ticker_code}.T"


def fetch_2year_data(ticker_code: str) -> pd.DataFrame:
    """yfinanceから2年分のデータを取得"""
    symbol = ticker_to_yahoo_symbol(ticker_code)

    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period="2y", auto_adjust=False)
        return hist
    except Exception as e:
        print(f"  [ERROR] {ticker_code}: データ取得失敗 - {e}")
        return pd.DataFrame()


def validate_dataframe(df: pd.DataFrame, ticker_code: str) -> ValidationResult:
    """DataFrameがスキーマに適合するか検証"""
    result = ValidationResult(ticker_code=ticker_code)

    if df.empty:
        result.success = False
        result.errors.append("データが空です")
        return result

    result.records_count = len(df)

    # 必須カラムの確認
    required_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        result.success = False
        result.errors.append(f"必須カラム欠損: {missing}")
        return result

    # Adj Closeの確認（オプション）
    if 'Adj Close' not in df.columns:
        result.warnings.append("Adj Closeカラムがありません（Closeを使用します）")

    # 日付形式の検証
    for idx in df.index:
        try:
            if hasattr(idx, 'strftime'):
                date_str = idx.strftime('%Y-%m-%d')
            else:
                date_str = str(idx)[:10]
            datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError:
            result.success = False
            result.errors.append(f"不正な日付形式: {idx}")
            break

    # 数値型の検証
    numeric_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
    for col in numeric_columns:
        if col in df.columns:
            non_numeric = df[col].apply(lambda x: not (pd.isna(x) or isinstance(x, (int, float))))
            if non_numeric.any():
                result.success = False
                result.errors.append(f"{col}に数値以外の値があります")

    # NULL値の確認
    null_counts = df[required_columns].isnull().sum()
    for col, count in null_counts.items():
        if count > 0:
            result.warnings.append(f"{col}に{count}件のNULL値")

    # 重複日付の確認
    if df.index.duplicated().any():
        dup_dates = df.index[df.index.duplicated()].tolist()
        result.success = False
        result.errors.append(f"重複日付: {dup_dates[:5]}...")

    return result


def save_to_csv(df: pd.DataFrame, ticker_code: str) -> bool:
    """DataFrameをCSVファイルとして保存"""
    CSV_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = CSV_DIR / f"{ticker_code}.csv"

    try:
        # インデックス（日付）を列として保存
        df_export = df.copy()
        df_export.index.name = 'Date'
        df_export.reset_index(inplace=True)
        df_export['Date'] = df_export['Date'].apply(
            lambda x: x.strftime('%Y-%m-%d') if hasattr(x, 'strftime') else str(x)[:10]
        )
        df_export.to_csv(csv_path, index=False, encoding='utf-8')
        return True
    except Exception as e:
        print(f"  [ERROR] CSV保存失敗: {e}")
        return False


def test_db_insert(df: pd.DataFrame, ticker_code: str) -> tuple[bool, Optional[str]]:
    """テスト用DBに挿入して検証"""
    if df.empty:
        return False, "データが空"

    # テスト用DBを初期化
    TEST_DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(TEST_DB_PATH)
    try:
        # スキーマを適用
        with open(SCHEMA_PATH, 'r', encoding='utf-8') as f:
            conn.executescript(f.read())

        # テスト用に銘柄マスタに登録
        conn.execute("""
            INSERT OR IGNORE INTO companies (ticker_code, company_name)
            VALUES (?, ?)
        """, (ticker_code, f"Test Company {ticker_code}"))

        # 株価データを挿入
        records = []
        for date, row in df.iterrows():
            trade_date = date.strftime('%Y-%m-%d') if hasattr(date, 'strftime') else str(date)[:10]
            adj_close = row.get('Adj Close', row['Close'])

            records.append((
                ticker_code,
                trade_date,
                float(row['Open']) if pd.notna(row['Open']) else None,
                float(row['High']) if pd.notna(row['High']) else None,
                float(row['Low']) if pd.notna(row['Low']) else None,
                float(row['Close']) if pd.notna(row['Close']) else None,
                int(row['Volume']) if pd.notna(row['Volume']) else 0,
                float(adj_close) if pd.notna(adj_close) else None
            ))

        conn.executemany("""
            INSERT OR REPLACE INTO daily_prices
            (ticker_code, trade_date, open_price, high_price, low_price, close_price, volume, adjusted_close)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, records)

        conn.commit()

        # 挿入結果を確認
        cursor = conn.execute(
            "SELECT COUNT(*) FROM daily_prices WHERE ticker_code = ?",
            (ticker_code,)
        )
        count = cursor.fetchone()[0]

        if count != len(records):
            return False, f"挿入件数不一致: 期待{len(records)}, 実際{count}"

        return True, None

    except sqlite3.Error as e:
        return False, f"SQLiteエラー: {e}"
    finally:
        conn.close()


def run_validation(tickers: list, dry_run: bool = False, sleep_interval: float = 0.5):
    """検証を実行"""
    print("=" * 60)
    print("株価データスキーマ検証")
    print(f"対象銘柄数: {len(tickers)}")
    print(f"CSV保存先: {CSV_DIR}")
    print(f"dry-run: {dry_run}")
    print("=" * 60)

    results: list[ValidationResult] = []

    for i, ticker_code in enumerate(tickers, 1):
        print(f"\n[{i}/{len(tickers)}] {ticker_code}...", end=" ")

        # データ取得
        df = fetch_2year_data(ticker_code)

        if df.empty:
            result = ValidationResult(
                ticker_code=ticker_code,
                success=False,
                errors=["データ取得失敗"]
            )
            results.append(result)
            print("SKIP (データなし)")
            continue

        # スキーマ検証
        result = validate_dataframe(df, ticker_code)

        # CSV保存
        if save_to_csv(df, ticker_code):
            result.csv_saved = True
            print(f"{result.records_count}件", end="")
        else:
            result.csv_saved = False
            result.errors.append("CSV保存失敗")

        # DB挿入テスト
        if not dry_run and result.success:
            db_ok, db_error = test_db_insert(df, ticker_code)
            result.db_inserted = db_ok
            if not db_ok:
                result.success = False
                result.errors.append(db_error)

        # 結果表示
        status = "OK" if result.success else "NG"
        print(f" [{status}]", end="")
        if result.warnings:
            print(f" (警告: {len(result.warnings)}件)", end="")
        if result.errors:
            print(f" (エラー: {result.errors})", end="")
        print()

        results.append(result)
        time.sleep(sleep_interval)

    # サマリー出力
    print_summary(results)

    # テスト用DBを削除
    if TEST_DB_PATH.exists():
        TEST_DB_PATH.unlink()

    return results


def print_summary(results: list[ValidationResult]):
    """検証サマリーを出力"""
    print("\n" + "=" * 60)
    print("検証サマリー")
    print("=" * 60)

    success_count = sum(1 for r in results if r.success)
    fail_count = len(results) - success_count
    total_records = sum(r.records_count for r in results)

    print(f"総銘柄数: {len(results)}")
    print(f"  成功: {success_count}")
    print(f"  失敗: {fail_count}")
    print(f"総レコード数: {total_records:,}")

    if fail_count > 0:
        print("\n失敗銘柄:")
        for r in results:
            if not r.success:
                print(f"  {r.ticker_code}: {r.errors}")

    # 全体の警告
    all_warnings = []
    for r in results:
        if r.warnings:
            all_warnings.extend([(r.ticker_code, w) for w in r.warnings])

    if all_warnings:
        print(f"\n警告 ({len(all_warnings)}件):")
        for ticker, warning in all_warnings[:10]:
            print(f"  {ticker}: {warning}")
        if len(all_warnings) > 10:
            print(f"  ... 他 {len(all_warnings) - 10}件")

    print("\n" + "=" * 60)
    if fail_count == 0:
        print("全銘柄のスキーマ検証に成功しました")
    else:
        print(f"{fail_count}銘柄でエラーが発生しました")
    print("=" * 60)


def main():
    parser = argparse.ArgumentParser(description='株価データのスキーマ検証')
    parser.add_argument('--ticker', '-t', help='検証する銘柄（カンマ区切りで複数指定可）')
    parser.add_argument('--dry-run', action='store_true', help='CSV保存のみでDB挿入しない')
    parser.add_argument('--sleep', type=float, default=0.3, help='API呼び出し間隔（秒）')
    args = parser.parse_args()

    # 対象銘柄を決定
    if args.ticker:
        tickers = [t.strip() for t in args.ticker.split(',')]
    else:
        tickers = get_all_tickers()
        if not tickers:
            print("銘柄マスタが空です。先に銘柄を登録してください。")
            print("または --ticker オプションで銘柄を指定してください。")
            return

    run_validation(tickers, dry_run=args.dry_run, sleep_interval=args.sleep)


if __name__ == "__main__":
    main()
