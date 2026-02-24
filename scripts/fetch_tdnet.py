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
import json
import re
import shutil
import sys
import time
import unicodedata
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, Any, List
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

# プロジェクトのベースディレクトリ
BASE_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(BASE_DIR / "lib"))

# XBRL解析関数を fetch_financials.py から再利用
from fetch_financials import (
    parse_ixbrl_financials,
    parse_ixbrl_forecast,
    _extract_forecast_fiscal_year,
    extract_edinet_zip,
    _wareki_to_seireki,
)

from db_utils import (
    insert_financial,
    insert_management_forecast,
    insert_announcement,
    log_batch_start, log_batch_end,
    is_valid_ticker_code,
    ticker_exists
)
from path_utils import find_edinet_temp_dir


# ============================================
# 定数
# ============================================

# TDnet URL
TDNET_BASE_URL = "https://www.release.tdnet.info/inbs/"
TDNET_MAIN_PAGE = "I_main_00.html"

# キャッシュディレクトリ
TDNET_XBRL_CACHE_DIR = BASE_DIR / "data" / "tdnet_xbrl_cache"

# レート制限
TDNET_REQUEST_SLEEP = 0.5  # 秒

# メタデータ抽出パターン（キャッシュ再投入用）
TICKER_RE = re.compile(r'tse-[^-]+-([0-9A-Z]{4,5}0)-')
JP_DATE_RE = re.compile(r'(\d{4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日')
ATTACHMENT_DATE_RE = re.compile(
    r'tse-[^-]+-([0-9A-Z]{4,5}0?)-(\d{4}-\d{2}-\d{2})-\d{2}-(\d{4}-\d{2}-\d{2})'
)

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


def compute_fiscal_end_date(fiscal_year_end: str, fiscal_quarter: str) -> Optional[str]:
    """
    FY末日と四半期から各四半期の期末日を計算する。

    FY末日からの逆算で各四半期の期末日を算出する。
    - FY/Q4: 0ヶ月前（FY末そのもの）
    - Q3: 3ヶ月前
    - Q2: 6ヶ月前
    - Q1: 9ヶ月前

    Args:
        fiscal_year_end: FY末日（YYYY-MM-DD形式、例: "2026-03-31"）
        fiscal_quarter: 四半期（例: "Q1", "Q2", "Q3", "FY"）

    Returns:
        fiscal_end_date（YYYY-MM-DD形式）またはNone

    Examples:
        >>> compute_fiscal_end_date("2026-03-31", "Q3")
        "2025-12-31"

        >>> compute_fiscal_end_date("2026-03-31", "Q1")
        "2025-06-30"
    """
    if not fiscal_year_end or not re.fullmatch(r'\d{4}-\d{2}-\d{2}', fiscal_year_end):
        return None

    fy_end_year = int(fiscal_year_end[:4])
    fy_end_month = int(fiscal_year_end[5:7])

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


def detect_fiscal_end_date_from_title(title: str, fiscal_year: str, fiscal_quarter: str) -> Optional[str]:
    """
    決算短信タイトルからfiscal_end_dateを推定する（iXBRL解析失敗時のフォールバック）

    タイトルの「YYYY年M月期」パターンから決算期末月を抽出し、
    compute_fiscal_end_date()でfiscal_quarterに応じた期末日を計算。

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
    fy_last_day = calendar.monthrange(fy_end_year, fy_end_month)[1]
    fy_end = f"{fy_end_year:04d}-{fy_end_month:02d}-{fy_last_day:02d}"

    return compute_fiscal_end_date(fy_end, fiscal_quarter)


# ============================================
# キャッシュ再投入用メタデータ抽出
# ============================================

def _normalize_jp_date(text: str) -> Optional[str]:
    """和文日付(2026年２月13日)をYYYY-MM-DDに変換。NFKC正規化で全角数字対応"""
    if not text:
        return None
    normalized = unicodedata.normalize('NFKC', text)
    m = JP_DATE_RE.search(normalized)
    if not m:
        # 既にISO形式ならそのまま返す
        return text.strip() if re.match(r'\d{4}-\d{2}-\d{2}$', text.strip()) else None
    y, mo, d = m.groups()
    return f"{int(y):04d}-{int(mo):02d}-{int(d):02d}"


def _get_ticker_from_namelist(namelist: list) -> Optional[str]:
    """ZIPのnamelist内ファイル名パターンからticker抽出（展開不要で高速）"""
    for name in namelist:
        m = TICKER_RE.search(Path(name).name)
        if m:
            raw = m.group(1)  # 例: 72030, 130A0
            return raw[:-1]   # チェックデジット除去
    return None


def _get_ticker_from_zip_path(zip_path: Path) -> Optional[str]:
    """ZIPファイルパスからticker抽出（展開不要で高速フィルタ用）"""
    try:
        with zipfile.ZipFile(zip_path) as zf:
            return _get_ticker_from_namelist(zf.namelist())
    except (zipfile.BadZipFile, Exception):
        return None


def _extract_filing_date_from_namelist(namelist: list) -> Optional[str]:
    """ZIPのnamelist内のAttachmentファイル名からfiling_date(YYYY-MM-DD)を抽出"""
    for name in namelist:
        m = ATTACHMENT_DATE_RE.search(name)
        if m:
            return m.group(3)  # filing_date
    return None


def extract_metadata_from_summary(summary_html: str) -> Dict[str, Any]:
    """Summary iXBRLのHTMLからメタデータをregex抽出（bs4不使用で高速）

    Returns:
        {
            'ticker_code': str or None,     # SecuritiesCode由来
            'fiscal_year': str or None,     # FiscalYearEnd年部分
            'fiscal_quarter': str or None,  # Q1-Q3 or FY
            'fiscal_year_end': str or None, # YYYY-MM-DD
            'announcement_date': str or None, # FilingDate ISO化
            'document_name': str or None,   # タイトル
        }
    """
    def _pick(tag_name: str) -> Optional[str]:
        m = re.search(
            rf'name=["\']({re.escape(tag_name)})["\'][^>]*>(.*?)</ix:(?:nonnumeric|nonfraction)>',
            summary_html, re.IGNORECASE | re.DOTALL
        )
        if not m:
            return None
        return re.sub(r'<[^>]+>', '', m.group(2)).strip()

    fy_end = _pick('tse-ed-t:FiscalYearEnd')
    qp = _pick('tse-ed-t:QuarterlyPeriod')
    doc = _pick('tse-ed-t:DocumentName')
    filing = _normalize_jp_date(_pick('tse-ed-t:FilingDate'))
    sec_code = _pick('tse-ed-t:SecuritiesCode')

    # fiscal_quarter判定
    if qp and qp.strip() in ('1', '2', '3'):
        fq = f'Q{qp.strip()}'
    elif doc:
        normalized_doc = unicodedata.normalize('NFKC', doc)
        qm = re.search(r'第([1-4])四半期', normalized_doc)
        if qm:
            fq = f'Q{qm.group(1)}'
        else:
            fq = 'FY'
    else:
        fq = 'FY'

    # fiscal_year判定
    fy = None
    if fy_end and re.match(r'\d{4}-\d{2}-\d{2}$', fy_end):
        fy = fy_end[:4]

    # ticker_code: SecuritiesCodeから取得（4桁 or 5桁→末尾除去）
    ticker = None
    if sec_code:
        sec_clean = sec_code.strip()
        if len(sec_clean) == 5 and sec_clean[-1] == '0':
            ticker = sec_clean[:-1]
        elif len(sec_clean) == 4:
            ticker = sec_clean

    return {
        'ticker_code': ticker,
        'fiscal_year': fy,
        'fiscal_quarter': fq,
        'fiscal_year_end': fy_end,
        'announcement_date': filing,
        'document_name': doc,
    }


# DEI TypeOfCurrentPeriodDEI → fiscal_quarter マッピング
DEI_PERIOD_MAP = {'Q1': 'Q1', 'Q2': 'Q2', 'Q3': 'Q3', 'Q4': 'Q4', 'FY': 'FY', 'HY': 'Q2'}


def _pick_ix_value(html: str, tag: str) -> Optional[str]:
    """iXBRL HTML内のix:nonNumeric/nonFractionタグからテキスト値を抽出

    name属性のシングル/ダブルクオート両対応、タグ名の大文字小文字を無視。
    """
    m = re.search(
        rf'<ix:(?:nonnumeric|nonfraction)\b[^>]*\bname=["\']({re.escape(tag)})["\'][^>]*>(.*?)</ix:(?:nonnumeric|nonfraction)>',
        html, re.IGNORECASE | re.DOTALL,
    )
    if not m:
        return None
    val = re.sub(r'<[^>]+>', '', m.group(2)).strip()
    return val or None


def _extract_metadata_from_attachment(zf: 'zipfile.ZipFile', namelist: list) -> Optional[Dict[str, Any]]:
    """Attachmentファイル名+iXBRL DEI要素からメタデータ抽出（Summary無しZIPのフォールバック）

    Attachmentファイル名例:
    0102010-qcpl11-tse-qcedjpfr-39090-2025-12-31-01-2026-02-13-ixbrl.htm
                                ^^^^^  ^^^^^^^^^^     ^^^^^^^^^^
                                code   fiscal_end     filing_date

    iXBRL DEI要素から取得:
    - jpdei_cor:TypeOfCurrentPeriodDEI → Q1/Q2/Q3/Q4/FY/HY
    - jpdei_cor:CurrentFiscalYearEndDateDEI → YYYY-MM-DD

    DEI要素が複数ファイルに分散している場合も走査して集約する。
    """
    ticker = _get_ticker_from_namelist(namelist)
    if not ticker:
        return None

    fq = None
    fy = None
    dei_fy_end = None
    filing_date = None
    first_fiscal_end = None

    for name in namelist:
        m = ATTACHMENT_DATE_RE.search(name)
        if not m or not name.endswith('-ixbrl.htm'):
            continue

        if filing_date is None:
            filing_date = m.group(3)
            first_fiscal_end = m.group(2)

        # iXBRL本文からDEI要素を読み取り
        html = zf.read(name).decode('utf-8', errors='ignore')

        if not fq:
            dei_type = _pick_ix_value(html, 'jpdei_cor:TypeOfCurrentPeriodDEI')
            fq = DEI_PERIOD_MAP.get(dei_type)

        if not dei_fy_end:
            dei_fy_end = _pick_ix_value(html, 'jpdei_cor:CurrentFiscalYearEndDateDEI')
            if dei_fy_end and re.match(r'\d{4}-\d{2}-\d{2}$', dei_fy_end):
                fy = dei_fy_end[:4]

        # 両方取れたら早期終了
        if fq and fy:
            break

    if filing_date is None:
        return None

    # DEIが取れない場合はtaxonomy prefixフォールバック
    if not fq:
        for name in namelist:
            taxonomy_match = re.search(r'tse-([aqs])[cn]', name)
            if taxonomy_match:
                prefix = taxonomy_match.group(1)
                if prefix == 'a':
                    fq = 'FY'
                elif prefix == 's':
                    fq = 'Q2'
                break

    # fiscal_year: DEIのfy_end年を使用、なければFY時のみfiscal_end_dateの年
    if not fy and fq == 'FY' and first_fiscal_end:
        fy = first_fiscal_end[:4]

    return {
        'ticker_code': ticker,
        'fiscal_year': fy,
        'fiscal_quarter': fq,
        'fiscal_year_end': dei_fy_end,
        'announcement_date': filing_date,
        'document_name': None,
    }


# ============================================
# TDnet クライアント
# ============================================

class TdnetClient:
    """TDnet HTMLスクレイピングクライアント"""

    def __init__(self, xbrl_cache_dir: Path = None):
        """
        Args:
            xbrl_cache_dir: XBRLキャッシュディレクトリ
        """
        self.session = requests.Session()
        self.xbrl_cache_dir = xbrl_cache_dir or TDNET_XBRL_CACHE_DIR
        self.xbrl_cache_dir.mkdir(parents=True, exist_ok=True)

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
        TDnetページをフェッチ（キャッシュなし、常にHTTPリクエスト）

        Args:
            page_url: ページファイル名（例: I_list_001_20240510.html）

        Returns:
            BeautifulSoup: 解析済みHTML
        """
        url = TDNET_BASE_URL + page_url
        print(f"  [DEBUG] HTTPリクエスト開始: {url}")
        try:
            response = self.session.get(url, timeout=10)
            print(f"  [DEBUG] レスポンス受信: {response.status_code}")
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
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

    def download_xbrl_zip(self, zip_url: str, announcement_date: str = None) -> Optional[bytes]:
        """
        XBRL ZIPファイルをダウンロード（日付フォルダキャッシュ対応）

        Args:
            zip_url: ZIPファイルのURL
            announcement_date: 発表日（YYYY-MM-DD）。指定時は日付フォルダに保存

        Returns:
            ZIPファイルのバイト列（失敗時はNone）
        """
        # URLからファイル名を抽出
        parsed_url = urlparse(zip_url)
        zip_filename = Path(parsed_url.path).name

        # キャッシュチェック: 日付フォルダ → フラット(レガシー)の順
        cache_paths = []
        if announcement_date:
            cache_paths.append(self.xbrl_cache_dir / announcement_date / zip_filename)
        cache_paths.append(self.xbrl_cache_dir / zip_filename)  # レガシーフラット

        for cache_path in cache_paths:
            if cache_path.exists():
                print(f"    [CACHE] キャッシュ使用: {cache_path.relative_to(self.xbrl_cache_dir)}")
                try:
                    content = cache_path.read_bytes()
                    # レガシーフラットキャッシュを日付フォルダに昇格
                    if announcement_date and cache_path.parent == self.xbrl_cache_dir:
                        dated_path = self.xbrl_cache_dir / announcement_date / zip_filename
                        try:
                            dated_path.parent.mkdir(parents=True, exist_ok=True)
                            shutil.move(str(cache_path), str(dated_path))
                            print(f"    [CACHE] レガシー→日付フォルダに昇格: {dated_path.relative_to(self.xbrl_cache_dir)}")
                        except Exception as e:
                            print(f"    [WARN] レガシーキャッシュ昇格失敗: {e}")
                    return content
                except Exception as e:
                    print(f"    [WARN] キャッシュ読み込み失敗、再ダウンロードします: {e}")
                    cache_path.unlink(missing_ok=True)

        # HTTPリクエスト
        try:
            print(f"    [HTTP] ダウンロード開始: {zip_url}")
            response = self.session.get(zip_url, timeout=60)
            response.raise_for_status()
            time.sleep(TDNET_REQUEST_SLEEP)

            # キャッシュに保存（日付フォルダ優先）
            if announcement_date:
                save_path = self.xbrl_cache_dir / announcement_date / zip_filename
            else:
                save_path = self.xbrl_cache_dir / zip_filename
            try:
                save_path.parent.mkdir(parents=True, exist_ok=True)
                save_path.write_bytes(response.content)
                print(f"    [CACHE] 保存完了: {save_path.relative_to(self.xbrl_cache_dir)}")
            except Exception as e:
                print(f"    [WARN] キャッシュ保存失敗: {e}")

            return response.content
        except Exception as e:
            print(f"  [ERROR] ZIPダウンロード失敗 ({zip_url}): {e}")
            return None


# ============================================
# メイン処理関数
# ============================================


def _process_zip_to_db(
    zip_content: bytes, ticker_code: str, fiscal_year: str, fiscal_quarter: str,
    title: Optional[str] = None, announcement_date: str = None, announcement_time: str = None,
    fiscal_year_end: Optional[str] = None,
) -> bool:
    """ZIPバイト→展開→パース→fiscal_end_date検証→DB投入の共通処理

    HTML経路（process_tdnet_announcement）とキャッシュ経路（process_cached_zip）の
    両方から呼ばれる。

    Args:
        zip_content: ZIPファイルのバイト列
        ticker_code: 証券コード
        fiscal_year: 決算年度
        fiscal_quarter: 四半期（Q1/Q2/Q3/FY）
        title: 決算短信タイトル（キャッシュ経路ではNoneの場合あり）
        announcement_date: 発表日（YYYY-MM-DD）
        announcement_time: 発表時刻
        fiscal_year_end: FY末日（YYYY-MM-DD形式、Summary iXBRLのtse-ed-t:FiscalYearEnd）

    Returns:
        保存成功時 True
    """
    extracted_paths = extract_edinet_zip(zip_content)
    if not extracted_paths:
        return False

    temp_dir = find_edinet_temp_dir(extracted_paths)

    try:
        print(f"    パーサー: XBRLP (iXBRL)")
        financials = parse_ixbrl_financials(extracted_paths)

        if not financials:
            print(f"    [WARN] 財務データを抽出できませんでした")
            return False

        xbrl_fiscal_end = financials.pop('fiscal_end_date', None)

        # fiscal_end_date検証・補正
        if fiscal_quarter in ('Q1', 'Q2', 'Q3'):
            # フォールバック1: タイトルの「YYYY年M月期」パターンから計算
            title_fiscal_end = None
            if title:
                title_fiscal_end = detect_fiscal_end_date_from_title(title, fiscal_year, fiscal_quarter)

            # フォールバック2: FiscalYearEnd（FY末日）+ QuarterlyPeriodから計算
            computed_fiscal_end = None
            if fiscal_year_end:
                computed_fiscal_end = compute_fiscal_end_date(fiscal_year_end, fiscal_quarter)

            # 補正候補: タイトル推定 > FiscalYearEnd計算 の優先度
            corrected = title_fiscal_end or computed_fiscal_end

            if xbrl_fiscal_end and corrected and xbrl_fiscal_end != corrected:
                source_label = "タイトル推定" if title_fiscal_end else "FiscalYearEnd計算"
                print(f"    [補正] fiscal_end_date: XBRL={xbrl_fiscal_end} → {source_label}={corrected}")
                xbrl_fiscal_end = corrected
            elif not xbrl_fiscal_end and corrected:
                source_label = "タイトル推定" if title_fiscal_end else "FiscalYearEnd計算"
                xbrl_fiscal_end = corrected
                print(f"    [補完] fiscal_end_date: {source_label}={xbrl_fiscal_end}")
        elif fiscal_quarter in ('FY', 'Q4'):
            if xbrl_fiscal_end:
                xbrl_fiscal_year = xbrl_fiscal_end[:4]
                if xbrl_fiscal_year != fiscal_year:
                    print(f"    [補正] fiscal_year: タイトル={fiscal_year} → XBRL={xbrl_fiscal_year}")
                    fiscal_year = xbrl_fiscal_year
            elif title:
                xbrl_fiscal_end = detect_fiscal_end_date_from_title(title, fiscal_year, fiscal_quarter)
                if xbrl_fiscal_end:
                    print(f"    [補完] fiscal_end_date: タイトルから推定={xbrl_fiscal_end}")

        # キャッシュ経路でtitleが無い場合: XBRLのfiscal_end_dateをそのまま使用
        if not xbrl_fiscal_end:
            print(f"    [WARN] fiscal_end_dateを特定できません: {ticker_code} {fiscal_year} {fiscal_quarter}")
            return False

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

        # --- 業績予想データの抽出・保存（実績保存の成否に依らず実行） ---
        forecast_saved = 0
        try:
            forecasts = parse_ixbrl_forecast(extracted_paths)
            if forecasts:
                forecast_fy = _extract_forecast_fiscal_year(extracted_paths)
                if forecast_fy:
                    for quarter, data in forecasts.items():
                        if any(v is not None for v in data.values()):
                            ok = insert_management_forecast(
                                ticker_code=ticker_code,
                                fiscal_year=forecast_fy,
                                fiscal_quarter=quarter,
                                announced_date=announcement_date,
                                forecast_type='initial',
                                source='TDnet',
                                **data
                            )
                            if ok:
                                forecast_saved += 1
                    if forecast_saved > 0:
                        print(f"    [予想] {forecast_saved}件保存: {ticker_code} → {forecast_fy}")
                else:
                    print(f"    [WARN] 予想の対象年度を特定できません")
        except Exception as e:
            print(f"    [WARN] 予想データ抽出失敗: {e}")

        return saved or forecast_saved > 0

    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def process_tdnet_announcement(client: TdnetClient, announcement: Dict[str, Any]) -> bool:
    """1つの決算短信を処理（HTML経路）

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

    if not ticker_exists(ticker_code):
        return False

    print(f"  処理中: {ticker_code} - {company_name}")
    print(f"    タイトル: {title}")

    fiscal_year, fiscal_quarter = detect_fiscal_period(title, announcement_date)
    print(f"    決算期: {fiscal_year} {fiscal_quarter}")

    zip_content = client.download_xbrl_zip(xbrl_zip_url, announcement_date=announcement_date)
    if not zip_content:
        return False

    return _process_zip_to_db(
        zip_content=zip_content,
        ticker_code=ticker_code,
        fiscal_year=fiscal_year,
        fiscal_quarter=fiscal_quarter,
        title=title,
        announcement_date=announcement_date,
        announcement_time=announcement_time,
    )


def _load_or_fetch_announcements(
    client, target_date: str, cache_date_dir: Path, force: bool = False
) -> Optional[List[Dict[str, Any]]]:
    """日次announcements一覧をJSONキャッシュ経由で取得。

    JSONキャッシュが存在すればそれを返し、なければTDnet HTMLを取得して
    JSONに保存してから返す。force=Trueの場合はキャッシュを無視して再取得。
    当日分は開示が追加される可能性があるため、JSONキャッシュを作成しない。

    Returns:
        announcements list on success, None on fetch failure (network error etc.)
    """
    manifest = cache_date_dir / "_announcements.json"
    is_today = target_date == datetime.now().strftime("%Y-%m-%d")

    if not force and not is_today and manifest.exists():
        try:
            return json.loads(manifest.read_text(encoding="utf-8"))
        except Exception:
            manifest.unlink(missing_ok=True)

    try:
        announcements = client.get_announcements(target_date)
    except Exception as e:
        print(f"  [ERROR] HTML取得失敗: {e}")
        return None

    # 過去日のみJSON保存（当日は開示追加の可能性があるためキャッシュしない）
    if not is_today:
        cache_date_dir.mkdir(parents=True, exist_ok=True)
        manifest.write_text(
            json.dumps(announcements, ensure_ascii=False), encoding="utf-8"
        )
    return announcements


def process_cached_zip(zip_path: Path, announcement_date: str, stats: Dict[str, int]) -> bool:
    """キャッシュZIPからメタデータ抽出→パース→DB投入

    Args:
        zip_path: ZIPファイルのパス
        announcement_date: 発表日（日付フォルダ名から確定済み）
        stats: 統計カウンタ {'processed': N, 'skipped_not_listed': N, 'failed': N}

    Returns:
        処理成功時 True
    """
    try:
        with zipfile.ZipFile(zip_path) as zf:
            namelist = zf.namelist()

            # Summary iXBRLからメタデータ抽出
            summary_files = [n for n in namelist if '/Summary/' in n and n.endswith('-ixbrl.htm')]
            metadata = None

            if summary_files:
                summary_html = zf.read(summary_files[0]).decode('utf-8', errors='ignore')
                metadata = extract_metadata_from_summary(summary_html)
                # ファイル名からtickerを補完
                if metadata and not metadata.get('ticker_code'):
                    metadata['ticker_code'] = _get_ticker_from_namelist(namelist)
            else:
                # Attachmentファイル名+iXBRL DEIからフォールバック抽出
                metadata = _extract_metadata_from_attachment(zf, namelist)

        if not metadata or not metadata.get('ticker_code'):
            print(f"    [WARN] メタデータ抽出失敗: {zip_path.name}")
            stats['failed'] = stats.get('failed', 0) + 1
            return False

        ticker_code = metadata['ticker_code']

        if not ticker_exists(ticker_code):
            stats['skipped_not_listed'] = stats.get('skipped_not_listed', 0) + 1
            return False

        fiscal_year = metadata.get('fiscal_year')
        fiscal_quarter = metadata.get('fiscal_quarter')
        title = metadata.get('document_name')

        if not fiscal_year or not fiscal_quarter:
            print(f"    [WARN] 決算期を特定できません: {zip_path.name} ({ticker_code})")
            stats['failed'] = stats.get('failed', 0) + 1
            return False

        # announcement_dateはフォルダ名優先、メタデータからの値はフォールバック
        ann_date = announcement_date or metadata.get('announcement_date')

        print(f"  処理中(キャッシュ): {ticker_code} {fiscal_year}{fiscal_quarter}")
        zip_content = zip_path.read_bytes()
        result = _process_zip_to_db(
            zip_content=zip_content,
            ticker_code=ticker_code,
            fiscal_year=fiscal_year,
            fiscal_quarter=fiscal_quarter,
            title=title,
            announcement_date=ann_date,
            fiscal_year_end=metadata.get('fiscal_year_end'),
        )

        if result:
            stats['processed'] = stats.get('processed', 0) + 1
        return result

    except (zipfile.BadZipFile, Exception) as e:
        print(f"    [ERROR] ZIP処理失敗: {zip_path.name}: {e}")
        stats['failed'] = stats.get('failed', 0) + 1
        return False


def fetch_tdnet_financials(days: int = 1, tickers: list = None,
                          date_from: str = None, date_to: str = None,
                          force: bool = False):
    """
    TDnet決算短信を取得

    Args:
        days: 過去N日分を取得（デフォルト1日）
        tickers: 対象銘柄リスト（None=全銘柄）
        date_from: 日付範囲開始（YYYY-MM-DD）
        date_to: 日付範囲終了（YYYY-MM-DD）
        force: JSONキャッシュを無視して再取得
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

        cache_stats = {'processed': 0, 'skipped_not_listed': 0, 'failed': 0}

        # 日付ごとに処理
        for target_date in date_list:
            print(f"\n[{target_date}]")

            cache_date_dir = TDNET_XBRL_CACHE_DIR / target_date

            announcements = _load_or_fetch_announcements(
                client, target_date, cache_date_dir, force=force
            )

            if announcements is None:
                # HTML取得失敗 → ZIPフォールバック（earningsのみ復旧）
                zip_files = sorted(cache_date_dir.glob('*.zip'))
                if zip_files:
                    if tickers:
                        tickers_set = set(tickers)
                        zip_files = [
                            z for z in zip_files
                            if _get_ticker_from_zip_path(z) in tickers_set
                        ]
                    print(f"  HTML取得失敗、キャッシュZIPから{len(zip_files)}件処理")
                    prev = cache_stats.get('processed', 0)
                    for zip_path in zip_files:
                        process_cached_zip(zip_path, target_date, cache_stats)
                    processed += cache_stats.get('processed', 0) - prev
                continue

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

                if not ticker_exists(ticker_code):
                    skipped_out_of_scope += 1
                    continue

                # 全announcements → announcements テーブルに保存（UPSERT）
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
                # download_xbrl_zip()が既存ZIPをキャッシュヒットするため、
                # JSONキャッシュ経由でもHTTP取得は発生しない
                if announcement_type == 'earnings' and announcement.get('xbrl_zip_url'):
                    if process_tdnet_announcement(client, announcement):
                        processed += 1

        log_batch_end(log_id, "success", processed)
        print("-" * 50)
        print(f"\n処理完了: 決算データ {processed}件保存, 適時開示 {announcements_saved}件保存")
        if skipped_out_of_scope > 0:
            print(f"JPXリスト外のためスキップ: {skipped_out_of_scope}件")
        if cache_stats.get('processed', 0) > 0:
            print(f"ZIPフォールバック: {cache_stats['processed']}件処理")
        if cache_stats.get('skipped_not_listed', 0) > 0:
            print(f"ZIPフォールバック: JPXリスト外スキップ {cache_stats['skipped_not_listed']}件")
        if cache_stats.get('failed', 0) > 0:
            print(f"ZIPフォールバック: 処理失敗 {cache_stats['failed']}件")

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
    parser.add_argument('--force', action='store_true',
                       help='JSONキャッシュを無視して再取得')
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
    fetch_tdnet_financials(
        days=days, tickers=tickers,
        date_from=date_from, date_to=date_to,
        force=args.force,
    )


if __name__ == "__main__":
    main()
