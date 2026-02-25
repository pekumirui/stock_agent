"""
EDINETコード更新スクリプト

EDINET APIから企業コードリストを取得し、
companiesテーブルのedinet_codeカラムを更新する

使用方法:
    python update_edinet_codes.py
    python update_edinet_codes.py --api-key YOUR_API_KEY
"""
import argparse
import os
import requests
from pathlib import Path
from typing import Optional, Dict, Any

from db_utils import (
    get_connection,
    log_batch_start,
    log_batch_end,
    is_valid_ticker_code
)
from env_utils import load_env


# EDINET APIエンドポイント
EDINET_API_BASE = "https://api.edinet-fsa.go.jp/api/v2"

BASE_DIR = Path(__file__).parent.parent


def fetch_edinet_codelist(api_key: Optional[str] = None, days: int = 90) -> Optional[Dict[str, Any]]:
    """
    EDINET APIのドキュメントリストから企業コードリストを収集

    過去N日分のドキュメントリストを取得し、
    EDINETコードと証券コードのマッピングを作成する

    Args:
        api_key: EDINET APIキー（任意）
        days: 過去何日分を取得するか（デフォルト90日）

    Returns:
        企業コードリスト（data形式）、失敗時はNone
    """
    from datetime import datetime, timedelta
    import time

    print(f"EDINET APIから企業コードリストを収集中（過去{days}日分）...")

    session = requests.Session()
    params = {'type': 2}  # メタデータのみ
    if api_key:
        params['Subscription-Key'] = api_key

    # EDINETコードと証券コードのマッピング
    company_map = {}  # {edinetCode: {secCode, filerName}}

    try:
        # 過去N日分のドキュメントリストを取得
        total_docs = 0
        docs_with_sec = 0

        for i in range(days):
            target_date = (datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d')
            url = f"{EDINET_API_BASE}/documents.json"
            params['date'] = target_date

            try:
                response = session.get(url, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()

                documents = data.get('results', [])
                total_docs += len(documents)

                # 各ドキュメントからEDINETコードと証券コードを抽出
                for doc in documents:
                    edinet_code = doc.get('edinetCode')
                    sec_code = doc.get('secCode')
                    filer_name = doc.get('filerName')

                    if edinet_code and sec_code:
                        docs_with_sec += 1
                        # 既存のマッピングがない場合のみ追加
                        if edinet_code not in company_map:
                            company_map[edinet_code] = {
                                'edinetCode': edinet_code,
                                'secCode': sec_code,
                                'filerName': filer_name
                            }

                # 進捗表示（10日ごと）
                if (i + 1) % 10 == 0:
                    print(f"  {i + 1}/{days}日処理... 収集: {len(company_map)}社 (ドキュメント: {total_docs}件, secCodeあり: {docs_with_sec}件)")

                # API制限対策
                time.sleep(0.5)

            except Exception as e:
                # 個別の日付でエラーが出ても続行
                print(f"  [WARN] {target_date}: {e}")
                continue

        print(f"収集完了: {len(company_map)}社")

        # data形式に変換
        return {'data': list(company_map.values())}

    except Exception as e:
        print(f"[ERROR] EDINET APIからの取得に失敗: {e}")
        return None


def parse_sec_code(sec_code: str) -> Optional[str]:
    """
    証券コードを正規化（4-5桁の数字）

    Args:
        sec_code: 証券コード（4桁 or 5桁）

    Returns:
        正規化された証券コード（4-5桁）、無効な場合はNone
    """
    if not sec_code:
        return None

    sec_code_clean = sec_code.strip()

    # EDINET APIは5桁形式（末尾0）で返すことがあるため、4桁に変換
    # 例: 79740 → 7974, 72030 → 7203, 369A0 → 369A
    if len(sec_code_clean) == 5 and sec_code_clean.endswith('0'):
        prefix = sec_code_clean[:4]
        # 4桁数字 OR 3桁数字+英字 のどちらかなら、末尾0を削除
        is_4digit = prefix.isdigit()
        is_3digit_alpha = (len(prefix) == 4 and
                          prefix[:3].isdigit() and
                          prefix[3].isalpha())

        if is_4digit or is_3digit_alpha:
            sec_code_clean = prefix

    # 4-5桁の数字、または4桁数字+英字1文字をチェック
    if is_valid_ticker_code(sec_code_clean):
        return sec_code_clean

    return None


def get_companies_without_edinet() -> set[str]:
    """
    EDINETコードが未登録の銘柄一覧を取得

    Returns:
        set[str]: EDINETコード未登録のティッカーコードのセット
    """
    with get_connection() as conn:
        cursor = conn.execute(
            "SELECT ticker_code FROM companies WHERE edinet_code IS NULL"
        )
        return {row['ticker_code'] for row in cursor.fetchall()}


def update_edinet_codes(data: Dict[str, Any]) -> tuple[int, int]:
    """
    取得した企業コードリストでDBを更新

    Args:
        data: EDINET APIのレスポンスJSON

    Returns:
        (更新件数, マッチ件数) のタプル
    """
    if not data or 'data' not in data:
        print("[ERROR] データが空です")
        return 0, 0

    companies = data['data']
    print(f"\n証券コードマッピング作成中...")

    # EDINETコード未登録の銘柄を事前取得
    missing_edinet_tickers = get_companies_without_edinet()
    print(f"[INFO] EDINETコード未登録の銘柄: {len(missing_edinet_tickers)}社")

    if not missing_edinet_tickers:
        print("[INFO] すべての銘柄にEDINETコードが登録済みです")
        return 0, 0

    updated = 0
    matched = 0
    skipped = 0
    progress_interval = 1000

    with get_connection() as conn:
        for i, company in enumerate(companies, 1):
            edinet_code = company.get('edinetCode')
            sec_code = company.get('secCode')
            company_name = company.get('filerName')

            # 証券コードまたはEDINETコードがない企業はスキップ
            if not sec_code or not edinet_code:
                skipped += 1
                continue

            # 証券コードを4桁に変換
            ticker_code = parse_sec_code(sec_code)
            if not ticker_code:
                skipped += 1
                continue

            # 未登録銘柄リストにない場合はスキップ
            if ticker_code not in missing_edinet_tickers:
                continue

            # DBに該当銘柄があるか確認
            cursor = conn.execute(
                "SELECT ticker_code FROM companies WHERE ticker_code = ?",
                (ticker_code,)
            )
            row = cursor.fetchone()

            if row:
                matched += 1

                # EDINETコードを更新（NULL判定は不要、事前フィルタ済み）
                conn.execute(
                    "UPDATE companies SET edinet_code = ?, updated_at = datetime('now', 'localtime') WHERE ticker_code = ?",
                    (edinet_code, ticker_code)
                )
                updated += 1

                # 更新したらセットから削除（重複防止）
                missing_edinet_tickers.discard(ticker_code)

            # 進捗表示
            if i % progress_interval == 0:
                print(f"  [{i}/{len(companies)}] 処理中... (マッチ: {matched}, 更新: {updated})")

        conn.commit()

    print(f"\n処理完了:")
    print(f"  有効な証券コード: {len(companies) - skipped}件")
    print(f"  スキップ（証券コードなし）: {skipped}件")
    print(f"  DBにマッチ: {matched}件")
    print(f"  EDINET更新: {updated}件")
    print(f"  未登録残り: {len(missing_edinet_tickers)}件")

    return updated, matched


def main():
    load_env()

    parser = argparse.ArgumentParser(description='EDINETコードを更新')
    parser.add_argument('--api-key', help='EDINET APIキー（未指定時は環境変数 EDINET_API_KEY）')
    parser.add_argument('--days', type=int, default=90, help='過去何日分のドキュメントから収集するか（デフォルト90日）')
    args = parser.parse_args()

    # APIキー: 引数 > 環境変数
    api_key = args.api_key or os.environ.get('EDINET_API_KEY')

    print("EDINETコード更新開始")
    print("-" * 50)

    log_id = log_batch_start("update_edinet_codes")

    try:
        # EDINETコードリストを収集
        data = fetch_edinet_codelist(api_key, days=args.days)

        if not data:
            log_batch_end(log_id, "failed", 0, "EDINET APIからのデータ取得に失敗")
            return

        # DBを更新
        updated, matched = update_edinet_codes(data)

        log_batch_end(log_id, "success", updated)

        print("-" * 50)
        print(f"完了: {updated}件のEDINETコードを更新")

        # 確認
        with get_connection() as conn:
            cursor = conn.execute(
                "SELECT COUNT(*) as total, COUNT(edinet_code) as with_edinet FROM companies"
            )
            row = cursor.fetchone()
            print(f"\n銘柄マスタ総数: {row['total']}銘柄")
            print(f"EDINETコード登録済: {row['with_edinet']}銘柄")

    except Exception as e:
        log_batch_end(log_id, "failed", 0, str(e))
        print(f"\n[ERROR] バッチ失敗: {e}")
        raise


if __name__ == "__main__":
    main()
