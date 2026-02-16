"""
決算短信取得バッチ - TDnet Webスクレイピング

TDnetから決算短信のXBRLを取得し、決算情報をDBに保存する

使用方法:
    python fetch_tdnet.py                           # 本日分のTDnet決算短信を取得
    python fetch_tdnet.py --days 7                  # 過去7日分
    python fetch_tdnet.py --ticker 7203,6758        # 特定銘柄のみ
    python fetch_tdnet.py --date-from 2024-02-01 --date-to 2024-02-05  # 日付範囲指定
"""
import argparse
import calendar
import re
import shutil
import sys
import tempfile
import time
import unicodedata
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, Any, List

import requests
from bs4 import BeautifulSoup

# プロジェクトのベースディレクトリ
BASE_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(BASE_DIR / "lib"))

# XBRL解析関数を fetch_financials.py から再利用
from fetch_financials import (
    parse_ixbrl_financials,
    extract_edinet_zip,
    _wareki_to_seireki,
)

from db_utils import (
    insert_financial,
    insert_announcement,
    log_batch_start, log_batch_end,
    is_valid_ticker_code,
    ticker_exists
)


# ============================================
# 定数
# ============================================

# TDnet URL
TDNET_BASE_URL = "https://www.release.tdnet.info/inbs/"
TDNET_MAIN_PAGE = "I_main_00.html"

# キャッシュディレクトリ
TDNET_CACHE_DIR = BASE_DIR / "data" / "tdnet_cache"

# レート制限
TDNET_REQUEST_SLEEP = 0.5  # 秒

# 決算期判定パターン
FISCAL_YEAR_PATTERN = r'(\d{4})年.*?期'
QUARTER_PATTERN = r'第([1-4])四半期'
FULL_YEAR_KEYWORDS = ['通期', '本決算', '期末']


# ============================================
# 決算期判定関数
# ============================================

def detect_fiscal_period(title: str, announcement_date: str) -> tuple:
    """
    決算短信タイトルと発表日から決算期を判定

    Args:
        title: 決算短信タイトル（例: "2024年3月期 第1四半期決算短信"）
        announcement_date: 発表日（YYYY-MM-DD）

    Returns:
        (fiscal_year: str, fiscal_quarter: str)

    Examples:
        >>> detect_fiscal_period("2024年3月期 第1四半期決算短信", "2024-05-10")
        ("2024", "Q1")

        >>> detect_fiscal_period("2024年3月期 通期決算短信", "2024-05-10")
        ("2024", "FY")
    """
    # 全角数字→半角数字に正規化（TDnetは全角数字を使う場合がある）
    normalized_title = unicodedata.normalize('NFKC', title)
    normalized_title = _wareki_to_seireki(normalized_title)

    # 1. 年度を抽出
    fiscal_year = None
    year_match = re.search(FISCAL_YEAR_PATTERN, normalized_title)
    if year_match:
        fiscal_year = year_match.group(1)
    else:
        # フォールバック: 発表日の年を使用
        fiscal_year = announcement_date[:4]

    # 2. 四半期を判定
    fiscal_quarter = 'FY'  # デフォルトは通期

    # 通期キーワードチェック
    is_full_year = any(kw in normalized_title for kw in FULL_YEAR_KEYWORDS)
    if is_full_year:
        fiscal_quarter = 'FY'
    else:
        # 四半期パターンチェック
        quarter_match = re.search(QUARTER_PATTERN, normalized_title)
        if quarter_match:
            q_num = quarter_match.group(1)
            fiscal_quarter = f'Q{q_num}'
        # else: fiscal_quarter = 'FY' (デフォルト値を使用)

    return fiscal_year, fiscal_quarter


def detect_fiscal_end_date_from_title(title: str, fiscal_year: str, fiscal_quarter: str) -> Optional[str]:
    """
    決算短信タイトルからfiscal_end_dateを推定する（iXBRL解析失敗時のフォールバック）

    タイトルの「YYYY年M月期」パターンから決算期末月を抽出し、
    fiscal_quarterに応じた期末日を計算。

    計算方法: FY末からの逆算
    - FY/Q4: 0ヶ月前（FY末そのもの）
    - Q3: 3ヶ月前
    - Q2: 6ヶ月前
    - Q1: 9ヶ月前

    Args:
        title: 決算短信タイトル（例: "2024年3月期 第1四半期決算短信"）
        fiscal_year: 決算年度（例: "2024"）
        fiscal_quarter: 四半期（例: "Q1"）

    Returns:
        fiscal_end_date（YYYY-MM-DD形式）またはNone

    Examples:
        >>> detect_fiscal_end_date_from_title("2024年3月期 第1四半期決算短信", "2024", "Q1")
        "2023-06-30"

        >>> detect_fiscal_end_date_from_title("2024年3月期 通期決算短信", "2024", "FY")
        "2024-03-31"

        >>> detect_fiscal_end_date_from_title("2024年12月期 通期決算短信", "2024", "FY")
        "2024-12-31"
    """
    normalized = unicodedata.normalize('NFKC', title)
    normalized = _wareki_to_seireki(normalized)

    m = re.search(r'(\d{4})年(\d{1,2})月期', normalized)
    if not m:
        return None

    fy_end_year = int(m.group(1))
    fy_end_month = int(m.group(2))

    months_before_fy = {'FY': 0, 'Q4': 0, 'Q3': 3, 'Q2': 6, 'Q1': 9}
    mb = months_before_fy.get(fiscal_quarter)
    if mb is None:
        return None

    # FY末月から逆算して各四半期の期末月・年を算出
    total = fy_end_year * 12 + (fy_end_month - 1) - mb
    end_year = total // 12
    end_month = (total % 12) + 1

    last_day = calendar.monthrange(end_year, end_month)[1]
    return f"{end_year:04d}-{end_month:02d}-{last_day:02d}"


# ============================================
# TDnet クライアント
# ============================================

class TdnetClient:
    """TDnet HTMLスクレイピングクライアント"""

    def __init__(self, cache_dir: Path = None):
        """
        Args:
            cache_dir: HTMLキャッシュディレクトリ
        """
        self.session = requests.Session()
        self.cache_dir = cache_dir or TDNET_CACHE_DIR
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def get_announcements(self, date: str) -> List[Dict[str, Any]]:
        """
        指定日の適時開示一覧から決算短信をフィルタして返す

        Args:
            date: 対象日（YYYY-MM-DD）

        Returns:
            [{
                'ticker_code': '7203',
                'company_name': 'トヨタ自動車',
                'title': '2024年3月期 第1四半期決算短信',
                'announcement_date': '2024-05-10',
                'xbrl_zip_url': 'https://...',
            }, ...]
        """
        announcements = []

        # 日付を TDnet のフォーマットに変換（例: I_list_001_20240510.html）
        date_str = date.replace('-', '')
        page_file = f"I_list_001_{date_str}.html"
        print(f"  [DEBUG] ページファイル: {page_file}")

        # ページURLを取得（複数ページの可能性もある）
        page_urls = [page_file]
        processed_urls = set()

        for page_url in page_urls:
            # 無限ループ防止：同じURLを2回処理しない
            if page_url in processed_urls:
                print(f"  [DEBUG] スキップ（処理済み）: {page_url}")
                continue
            processed_urls.add(page_url)
            try:
                # HTMLを取得
                soup = self._fetch_page(page_url)
                if soup is None:
                    continue

                # テーブルを解析
                table = soup.find("table", {"id": "main-list-table"})
                if table is None:
                    # データがない日
                    continue

                rows = table.find_all('tr')
                if not rows:
                    continue

                # 各行を処理
                for row in rows:
                    announcement = self._parse_row(row, date)
                    if announcement:
                        announcements.append(announcement)

                # ページネーションチェック
                next_pages = self._get_pagination_urls(soup)
                print(f"  [DEBUG] ページネーション: {len(next_pages)}ページ追加")
                page_urls.extend(next_pages)

            except Exception as e:
                print(f"  [ERROR] ページ取得失敗 ({page_url}): {e}")
                continue

        return announcements

    def _fetch_page(self, page_url: str) -> Optional[BeautifulSoup]:
        """
        TDnetページをフェッチ（キャッシュ利用）

        Args:
            page_url: ページファイル名（例: I_list_001_20240510.html）

        Returns:
            BeautifulSoup: 解析済みHTML
        """
        cache_path = self.cache_dir / page_url

        # キャッシュチェック
        if cache_path.exists():
            print(f"  [DEBUG] キャッシュ使用: {page_url}")
            with open(cache_path, 'r', encoding='utf-8') as f:
                return BeautifulSoup(f.read(), 'html.parser')

        # HTTPリクエスト
        url = TDNET_BASE_URL + page_url
        print(f"  [DEBUG] HTTPリクエスト開始: {url}")
        try:
            response = self.session.get(url, timeout=10)
            print(f"  [DEBUG] レスポンス受信: {response.status_code}")
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')

            # キャッシュに保存
            with open(cache_path, 'w', encoding='utf-8') as f:
                f.write(soup.prettify())

            print(f"  [DEBUG] キャッシュ保存: {cache_path}")
            return soup

        except Exception as e:
            print(f"  [ERROR] HTTPリクエスト失敗 ({url}): {e}")
            return None

    def _parse_row(self, row: BeautifulSoup, announcement_date: str) -> Optional[Dict[str, Any]]:
        """
        テーブル行から適時開示情報を抽出

        決算短信だけでなく、業績予想の修正・配当なども取得対象とする。
        XBRL解析は決算短信のみ実施。

        Args:
            row: BeautifulSoup row要素
            announcement_date: 発表日（YYYY-MM-DD）

        Returns:
            適時開示情報の辞書（対象外の場合はNone）
        """
        # タイトルセル
        title_td = row.find('td', {'class': 'kjTitle'})
        if title_td is None:
            return None

        title = title_td.get_text().replace("\n", "").replace("\u3000", "").replace(" ", "")

        # 時刻を取得
        time_td = row.find('td', {'class': 'kjTime'})
        announcement_time = time_td.get_text().strip() if time_td else None

        # announcement_type判定
        if '決算短信' in title:
            announcement_type = 'earnings'
        elif '業績予想の修正' in title or '業績予想及び' in title:
            announcement_type = 'revision'
        elif '配当' in title:
            announcement_type = 'dividend'
        else:
            announcement_type = 'other'

        # XBRLセル
        xbrl_td = row.find('td', {'class': 'kjXbrl'})
        has_xbrl = xbrl_td is not None and 'XBRL' in xbrl_td.get_text()

        # 決算短信はXBRL必須、それ以外はタイトルで判定して取得
        if announcement_type == 'earnings' and not has_xbrl:
            return None

        # 決算短信・業績予想修正・配当のみ取得（その他はスキップ）
        if announcement_type == 'other':
            return None

        # 証券コード
        code_td = row.find('td', {'class': 'kjCode'})
        if code_td is None:
            return None

        # TDnetは証券コードに末尾チェックデジットを付加 (例: 72030, 285A0)
        code_text = code_td.get_text().replace("\n", "").replace("\u3000", "").replace(" ", "")
        if len(code_text) >= 5:
            ticker_code = code_text[:-1]
        else:
            ticker_code = code_text

        # 会社名
        name_td = row.find('td', {'class': 'kjName'})
        company_name = name_td.get_text().replace("\n", "").replace("\u3000", "").replace(" ", "") if name_td else ""

        # XBRL ZIP URL（決算短信のみ）
        xbrl_zip_url = None
        if has_xbrl and announcement_type == 'earnings' and xbrl_td is not None:
            zip_link = xbrl_td.find('a', {'class': 'style002'})
            if zip_link:
                zip_filename = zip_link['href']
                xbrl_zip_url = TDNET_BASE_URL + zip_filename

        # PDF URL
        document_url = None
        title_link = title_td.find('a')
        if title_link and title_link.get('href'):
            document_url = TDNET_BASE_URL + title_link['href']

        return {
            'ticker_code': ticker_code,
            'company_name': company_name,
            'title': title,
            'announcement_date': announcement_date,
            'announcement_time': announcement_time,
            'announcement_type': announcement_type,
            'xbrl_zip_url': xbrl_zip_url,
            'document_url': document_url,
        }

    def _get_pagination_urls(self, soup: BeautifulSoup) -> List[str]:
        """
        ページネーションURLを取得

        Args:
            soup: BeautifulSoup

        Returns:
            次ページのURLリスト
        """
        page_urls = []

        page_td = soup.find("td", {'class': 'pagerTd'})
        if page_td is None:
            return page_urls

        pages = page_td.find_all('div', {'class': 'pager-M'})
        for page in pages:
            onclick = page.get('onclick')
            if onclick:
                # onclick="pagerLink('I_list_002_20240510.html')" から URL を抽出
                page_url = onclick.replace("pagerLink", "").replace("(", "").replace(")", "").replace("'", "")
                page_urls.append(page_url)

        return page_urls

    def download_xbrl_zip(self, zip_url: str) -> Optional[bytes]:
        """
        XBRL ZIPファイルをダウンロード

        Args:
            zip_url: ZIPファイルのURL

        Returns:
            ZIPファイルのバイト列（失敗時はNone）
        """
        try:
            response = self.session.get(zip_url, timeout=60)
            response.raise_for_status()
            return response.content
        except Exception as e:
            print(f"  [ERROR] ZIPダウンロード失敗 ({zip_url}): {e}")
            return None


# ============================================
# メイン処理関数
# ============================================

def process_tdnet_announcement(client: TdnetClient, announcement: Dict[str, Any]) -> bool:
    """
    1つの決算短信を処理

    【NEW】JPXリスト外の銘柄を事前に除外

    Args:
        client: TdnetClientインスタンス
        announcement: 決算短信情報

    Returns:
        処理成功時 True
    """
    ticker_code = announcement['ticker_code']
    company_name = announcement['company_name']
    title = announcement['title']
    announcement_date = announcement['announcement_date']
    announcement_time = announcement.get('announcement_time')
    xbrl_zip_url = announcement.get('xbrl_zip_url')

    # 【NEW】事前チェック: JPXリスト（companiesテーブル）に登録されているか
    if not ticker_exists(ticker_code):
        # ログ出力なし（統計で集計するため）
        return False

    print(f"  処理中: {ticker_code} - {company_name}")
    print(f"    タイトル: {title}")

    # 決算期を判定
    fiscal_year, fiscal_quarter = detect_fiscal_period(title, announcement_date)
    print(f"    決算期: {fiscal_year} {fiscal_quarter}")

    # XBRL ZIP をダウンロード
    zip_content = client.download_xbrl_zip(xbrl_zip_url)
    if not zip_content:
        return False

    # ZIP を展開
    extracted_paths = extract_edinet_zip(zip_content)
    if not extracted_paths:
        return False

    # temp_dirを特定（クリーンアップ用）
    first_path = extracted_paths[0]
    temp_dir = first_path
    while temp_dir.parent != temp_dir and not str(temp_dir.name).startswith("edinet_"):
        temp_dir = temp_dir.parent
    if not str(temp_dir.name).startswith("edinet_"):
        temp_dir = first_path
        while temp_dir.parent != temp_dir and temp_dir.parent != Path(tempfile.gettempdir()):
            temp_dir = temp_dir.parent

    try:
        # XBRL を解析
        print(f"    パーサー: XBRLP (iXBRL)")
        financials = parse_ixbrl_financials(extracted_paths)

        if not financials:
            print(f"    [WARN] 財務データを抽出できませんでした")
            return False

        # iXBRL由来のfiscal_end_dateでfiscal_yearを補正
        xbrl_fiscal_end = financials.pop('fiscal_end_date', None)

        # 【NEW】四半期の場合、タイトルから推定した期末日と検証
        if fiscal_quarter in ('Q1', 'Q2', 'Q3'):
            title_fiscal_end = detect_fiscal_end_date_from_title(title, fiscal_year, fiscal_quarter)

            if xbrl_fiscal_end and title_fiscal_end and xbrl_fiscal_end != title_fiscal_end:
                # XBRL期末日とタイトル推定が不一致 → タイトル推定を優先
                print(f"    [補正] fiscal_end_date: XBRL={xbrl_fiscal_end} → タイトル推定={title_fiscal_end}")
                xbrl_fiscal_end = title_fiscal_end
            elif not xbrl_fiscal_end:
                # XBRL取得失敗 → タイトル推定を使用
                xbrl_fiscal_end = title_fiscal_end
                if xbrl_fiscal_end:
                    print(f"    [補完] fiscal_end_date: タイトルから推定={xbrl_fiscal_end}")
        else:
            # FY/Q4の場合はXBRL優先（会計年度末=期末なので正確）
            if xbrl_fiscal_end:
                xbrl_fiscal_year = xbrl_fiscal_end[:4]
                if xbrl_fiscal_year != fiscal_year:
                    print(f"    [補正] fiscal_year: タイトル={fiscal_year} → XBRL={xbrl_fiscal_year}")
                    fiscal_year = xbrl_fiscal_year
            else:
                # フォールバック: タイトルから推定
                xbrl_fiscal_end = detect_fiscal_end_date_from_title(title, fiscal_year, fiscal_quarter)
                if xbrl_fiscal_end:
                    print(f"    [補完] fiscal_end_date: タイトルから推定={xbrl_fiscal_end}")

        if not xbrl_fiscal_end:
            print(f"    [WARN] fiscal_end_dateを特定できません: {ticker_code} {fiscal_year} {fiscal_quarter}")
            return False  # fiscal_end_date必須のためスキップ

        # DBに保存（上書きチェックは insert_financial 内で実施）
        saved = insert_financial(
            ticker_code=ticker_code,
            fiscal_year=fiscal_year,
            fiscal_quarter=fiscal_quarter,
            fiscal_end_date=xbrl_fiscal_end,
            announcement_date=announcement_date,
            announcement_time=announcement_time,
            revenue=financials.get('revenue'),
            gross_profit=financials.get('gross_profit'),
            operating_income=financials.get('operating_income'),
            ordinary_income=financials.get('ordinary_income'),
            net_income=financials.get('net_income'),
            eps=financials.get('eps'),
            source='TDnet',
            edinet_doc_id=None
        )

        if saved:
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
                print(f"    [一部欠損] [{ticker_code} {fiscal_year}{fiscal_quarter}] {detail}（欠損: {', '.join(missing)}）")
            else:
                print(f"    保存完了: [{ticker_code} {fiscal_year}{fiscal_quarter}] {detail}")
            return True
        else:
            # スキップされた（EDINET データが既に存在）
            return False

    finally:
        # 一時ディレクトリを確実にクリーンアップ
        shutil.rmtree(temp_dir, ignore_errors=True)


def fetch_tdnet_financials(days: int = 1, tickers: list = None,
                          date_from: str = None, date_to: str = None):
    """
    TDnet決算短信を取得

    【NEW】JPXリスト外のスキップ件数を統計出力

    Args:
        days: 過去N日分を取得（デフォルト1日）
        tickers: 対象銘柄リスト（None=全銘柄）
        date_from: 日付範囲開始（YYYY-MM-DD）
        date_to: 日付範囲終了（YYYY-MM-DD）
    """
    log_id = log_batch_start("fetch_tdnet")
    processed = 0
    announcements_saved = 0
    skipped_out_of_scope = 0  # 【NEW】JPXリスト外のスキップ件数

    client = TdnetClient()

    print(f"TDnet決算短信取得開始")
    print("-" * 50)

    try:
        # 日付リストを生成
        if date_from and date_to:
            # 日付範囲指定
            start = datetime.strptime(date_from, '%Y-%m-%d')
            end = datetime.strptime(date_to, '%Y-%m-%d')
            date_list = []
            current = start
            while current <= end:
                date_list.append(current.strftime('%Y-%m-%d'))
                current += timedelta(days=1)
            print(f"対象期間: {date_from} ～ {date_to} ({len(date_list)}日間)")
        else:
            # 過去N日分
            date_list = []
            for i in range(days):
                target_date = (datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d')
                date_list.append(target_date)
            print(f"対象期間: 過去{days}日分")

        print("-" * 50)

        # 日付ごとに処理
        for target_date in date_list:
            print(f"\n[{target_date}]")

            # 決算短信一覧を取得
            announcements = client.get_announcements(target_date)

            if not announcements:
                print("  決算短信なし")
                continue

            print(f"  {len(announcements)}件の適時開示")

            # 銘柄フィルタ
            if tickers:
                announcements = [a for a in announcements if a['ticker_code'] in tickers]
                print(f"  フィルタ後: {len(announcements)}件")

            # 各適時開示を処理
            for announcement in announcements:
                ticker_code = announcement['ticker_code']
                announcement_type = announcement.get('announcement_type', 'earnings')

                # 【NEW】事前チェック
                if not ticker_exists(ticker_code):
                    skipped_out_of_scope += 1
                    continue

                # 全announcements → announcements テーブルに保存
                fiscal_year_ann, fiscal_quarter_ann = None, None
                if announcement_type == 'earnings':
                    fiscal_year_ann, fiscal_quarter_ann = detect_fiscal_period(
                        announcement['title'], announcement['announcement_date']
                    )
                insert_announcement(
                    ticker_code=ticker_code,
                    announcement_date=announcement['announcement_date'],
                    announcement_time=announcement.get('announcement_time'),
                    announcement_type=announcement_type,
                    title=announcement['title'],
                    fiscal_year=fiscal_year_ann,
                    fiscal_quarter=fiscal_quarter_ann,
                    document_url=announcement.get('document_url'),
                    source='TDnet'
                )
                announcements_saved += 1

                # 決算短信のみ: XBRL解析 + financials 投入
                if announcement_type == 'earnings' and announcement.get('xbrl_zip_url'):
                    if process_tdnet_announcement(client, announcement):
                        processed += 1

                    # レート制限（XBRL DL時のみ）
                    time.sleep(TDNET_REQUEST_SLEEP)

        log_batch_end(log_id, "success", processed)
        print("-" * 50)
        print(f"\n処理完了: 決算データ {processed}件保存, 適時開示 {announcements_saved}件保存")
        if skipped_out_of_scope > 0:
            print(f"JPXリスト外のためスキップ: {skipped_out_of_scope}件")

    except Exception as e:
        log_batch_end(log_id, "failed", processed, str(e))
        print(f"\n[ERROR] バッチ失敗: {e}")
        raise


# ============================================
# CLI エントリポイント
# ============================================

def main():
    parser = argparse.ArgumentParser(description='TDnetから決算短信を取得')
    parser.add_argument('--days', type=int, default=1,
                       help='過去N日分を取得（デフォルト1日）')
    parser.add_argument('--ticker', '-t',
                       help='特定銘柄のみ取得（カンマ区切り）')
    parser.add_argument('--date-from',
                       help='日付範囲開始（YYYY-MM-DD）')
    parser.add_argument('--date-to',
                       help='日付範囲終了（YYYY-MM-DD）')
    args = parser.parse_args()

    # バリデーション
    if args.date_from and args.date_to:
        # 日付範囲指定モード
        try:
            datetime.strptime(args.date_from, '%Y-%m-%d')
            datetime.strptime(args.date_to, '%Y-%m-%d')
        except ValueError:
            print("[ERROR] 日付フォーマットが不正です（YYYY-MM-DD）")
            return

        if args.date_from > args.date_to:
            print("[ERROR] date-from が date-to より後です")
            return

        date_from = args.date_from
        date_to = args.date_to
        days = None

    elif args.date_from or args.date_to:
        print("[ERROR] --date-from と --date-to は両方指定してください")
        return
    else:
        # 過去N日分モード
        date_from = None
        date_to = None
        days = args.days

    # 対象銘柄
    tickers = None
    if args.ticker:
        tickers = [t.strip() for t in args.ticker.split(',')]

    # バッチ実行
    fetch_tdnet_financials(days=days, tickers=tickers, date_from=date_from, date_to=date_to)


if __name__ == "__main__":
    main()
