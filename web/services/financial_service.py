"""決算データ取得・計算サービス"""
import sqlite3
import sys
from pathlib import Path
from typing import Optional

# scriptsディレクトリをパスに追加（db_utilsのimport用）
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "scripts"))
import db_utils


def get_db():
    """SQLite接続を取得"""
    conn = sqlite3.connect(str(db_utils.DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def get_viewer_data(
    target_date: str,
    types: list[str] | None = None,
    sort: str = "time",
    order: str = "desc",
) -> list[dict]:
    """
    指定日の決算発表一覧データを取得。

    QoQ計算: 単独四半期の前四半期比（v_financials_standalone_quarter使用）
    YoY計算: 累計ベースの前年同期比（v_financials_yoy使用）

    注意: announcementsテーブルが存在しない場合（Agent A未完了）は、
    financialsテーブルのannouncement_dateで代替する。
    """
    conn = get_db()
    try:
        rows = []

        # まずannouncementsテーブルの存在確認
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='announcements'"
        )
        has_announcements = cursor.fetchone() is not None

        announcement_rows = []

        if has_announcements:
            # announcementsテーブルからデータ取得
            query = """
                SELECT a.ticker_code, a.announcement_time, a.announcement_type, a.title,
                       a.fiscal_year, a.fiscal_quarter,
                       c.company_name
                FROM announcements a
                JOIN companies c ON a.ticker_code = c.ticker_code
                WHERE a.announcement_date = ?
            """
            params: list = [target_date]
            if types:
                placeholders = ",".join("?" * len(types))
                query += f" AND a.announcement_type IN ({placeholders})"
                params.extend(types)

            cursor = conn.execute(query, params)
            announcement_rows = cursor.fetchall()

        if not announcement_rows:
            # フォールバック: financialsテーブルから取得
            # (announcementsテーブルが存在しない場合、またはデータが空の場合)
            query = """
                SELECT f.ticker_code, NULL as announcement_time,
                       'earnings' as announcement_type,
                       NULL as title, f.fiscal_year, f.fiscal_quarter,
                       c.company_name
                FROM financials f
                JOIN companies c ON f.ticker_code = c.ticker_code
                WHERE f.announcement_date = ?
            """
            params = [target_date]
            cursor = conn.execute(query, params)
            announcement_rows = cursor.fetchall()

        for row in announcement_rows:
            ticker = row["ticker_code"]
            fy = row["fiscal_year"]
            fq = row["fiscal_quarter"]

            data = {
                "ticker_code": ticker,
                "company_name": row["company_name"],
                "announcement_time": row["announcement_time"] or "",
                "announcement_type": row["announcement_type"],
                "title": row["title"] or "",
                "fiscal_year": fy,
                "fiscal_quarter": fq,
                # QoQ, YoY, Consensus はNoneで初期化
                "revenue_qoq": None,
                "gross_profit_qoq": None,
                "operating_income_qoq": None,
                "ordinary_income_qoq": None,
                "net_income_qoq": None,
                "revenue_yoy": None,
                "gross_profit_yoy": None,
                "operating_income_yoy": None,
                "ordinary_income_yoy": None,
                "net_income_yoy": None,
                "revenue_con": None,
                "operating_income_con": None,
                "ordinary_income_con": None,
                "net_income_con": None,
                "dividend_info": None,
            }

            # YoY計算（v_financials_yoy ビューを使用）
            if fy and fq:
                yoy_cursor = conn.execute(
                    """
                    SELECT revenue_yoy_pct, gross_profit_yoy_pct,
                           operating_income_yoy_pct, net_income_yoy_pct,
                           ordinary_income, ordinary_income_prev_year
                    FROM v_financials_yoy
                    WHERE ticker_code = ? AND fiscal_year = ? AND fiscal_quarter = ?
                """,
                    [ticker, fy, fq],
                )
                yoy_row = yoy_cursor.fetchone()
                if yoy_row:
                    data["revenue_yoy"] = yoy_row["revenue_yoy_pct"]
                    data["gross_profit_yoy"] = yoy_row["gross_profit_yoy_pct"]
                    data["operating_income_yoy"] = yoy_row["operating_income_yoy_pct"]
                    data["net_income_yoy"] = yoy_row["net_income_yoy_pct"]
                    # 経常利益YoY（ビューにないので計算）
                    oi = yoy_row["ordinary_income"]
                    oi_prev = yoy_row["ordinary_income_prev_year"]
                    if oi is not None and oi_prev is not None and oi_prev != 0:
                        data["ordinary_income_yoy"] = round(
                            (oi - oi_prev) * 100.0 / abs(oi_prev), 1
                        )

            # QoQ計算（v_financials_standalone_quarter 使用 - Agent A作成予定）
            # ビューが存在しない場合はスキップ
            try:
                if fy and fq and fq != "FY":
                    # 前四半期の単独四半期と比較
                    prev_q_map = {
                        "Q2": ("Q1", fy),
                        "Q3": ("Q2", fy),
                        "Q4": ("Q3", fy),
                        "Q1": ("Q4", str(int(fy) - 1)),
                    }
                    prev_info = prev_q_map.get(fq)
                    if prev_info:
                        prev_fq, prev_fy_q = prev_info

                        cur_cursor = conn.execute(
                            """
                            SELECT revenue_standalone, gross_profit_standalone,
                                   operating_income_standalone, ordinary_income_standalone,
                                   net_income_standalone, has_prev_quarter
                            FROM v_financials_standalone_quarter
                            WHERE ticker_code = ? AND fiscal_year = ? AND fiscal_quarter = ?
                                  AND has_prev_quarter = 1
                        """,
                            [ticker, fy, fq],
                        )
                        cur_sq = cur_cursor.fetchone()

                        prev_cursor = conn.execute(
                            """
                            SELECT revenue_standalone, gross_profit_standalone,
                                   operating_income_standalone, ordinary_income_standalone,
                                   net_income_standalone, has_prev_quarter
                            FROM v_financials_standalone_quarter
                            WHERE ticker_code = ? AND fiscal_year = ? AND fiscal_quarter = ?
                                  AND has_prev_quarter = 1
                        """,
                            [ticker, prev_fy_q, prev_fq],
                        )
                        prev_sq = prev_cursor.fetchone()

                        if cur_sq and prev_sq:
                            for field, key in [
                                ("revenue_standalone", "revenue_qoq"),
                                ("gross_profit_standalone", "gross_profit_qoq"),
                                ("operating_income_standalone", "operating_income_qoq"),
                                ("ordinary_income_standalone", "ordinary_income_qoq"),
                                ("net_income_standalone", "net_income_qoq"),
                            ]:
                                cur_val = cur_sq[field]
                                prev_val = prev_sq[field]
                                if (
                                    cur_val is not None
                                    and prev_val is not None
                                    and prev_val != 0
                                ):
                                    data[key] = round(
                                        (cur_val - prev_val) * 100.0 / abs(prev_val), 1
                                    )
            except Exception:
                pass  # ビュー未作成時は無視

            # 配当情報（management_forecastsから取得 - Agent A作成予定）
            try:
                div_cursor = conn.execute(
                    """
                    SELECT dividend_per_share FROM management_forecasts
                    WHERE ticker_code = ? AND fiscal_year = ?
                    ORDER BY announced_date DESC LIMIT 1
                """,
                    [ticker, fy],
                )
                div_row = div_cursor.fetchone()
                if div_row and div_row["dividend_per_share"]:
                    data["dividend_info"] = (
                        f"合計{div_row['dividend_per_share']:.2f}円"
                    )
            except Exception:
                pass  # テーブル未作成時は無視

            rows.append(data)

        # ソート
        if sort == "time":
            rows.sort(
                key=lambda x: x.get("announcement_time") or "",
                reverse=(order == "desc"),
            )
        elif sort == "ticker":
            rows.sort(
                key=lambda x: x.get("ticker_code") or "",
                reverse=(order == "desc"),
            )

        return rows
    finally:
        conn.close()


def get_detail_data(ticker_code: str, target_date: str) -> dict:
    """展開行用の詳細データ取得"""
    conn = get_db()
    try:
        result = {
            "ticker_code": ticker_code,
            "cumulative": None,  # 累計実績
            "forecast": None,  # 会社予想（最新）
            "initial_forecast": None,  # 期初予想
            "q4_standalone": None,  # 4Q単体予想
        }

        # 該当発表の fiscal_year, fiscal_quarter を取得
        # announcementsテーブル or financials テーブルから
        fy, fq = None, None
        try:
            cursor = conn.execute(
                """
                SELECT fiscal_year, fiscal_quarter FROM announcements
                WHERE ticker_code = ? AND announcement_date = ?
                LIMIT 1
            """,
                [ticker_code, target_date],
            )
            row = cursor.fetchone()
            if row:
                fy, fq = row["fiscal_year"], row["fiscal_quarter"]
        except Exception:
            pass

        if not fy:
            cursor = conn.execute(
                """
                SELECT fiscal_year, fiscal_quarter FROM financials
                WHERE ticker_code = ? AND announcement_date = ?
                LIMIT 1
            """,
                [ticker_code, target_date],
            )
            row = cursor.fetchone()
            if row:
                fy, fq = row["fiscal_year"], row["fiscal_quarter"]

        if not fy:
            return result

        result["fiscal_year"] = fy
        result["fiscal_quarter"] = fq

        # 累計実績
        cursor = conn.execute(
            """
            SELECT revenue, gross_profit, operating_income, ordinary_income, net_income
            FROM financials
            WHERE ticker_code = ? AND fiscal_year = ? AND fiscal_quarter = ?
        """,
            [ticker_code, fy, fq],
        )
        cum_row = cursor.fetchone()
        if cum_row:
            # 前年同期の累計
            prev_fy = str(int(fy) - 1)
            prev_cursor = conn.execute(
                """
                SELECT revenue, gross_profit, operating_income, ordinary_income, net_income
                FROM financials
                WHERE ticker_code = ? AND fiscal_year = ? AND fiscal_quarter = ?
            """,
                [ticker_code, prev_fy, fq],
            )
            prev_cum = prev_cursor.fetchone()

            result["cumulative"] = _calc_changes(cum_row, prev_cum)

        # 会社予想（最新）
        try:
            cursor = conn.execute(
                """
                SELECT revenue, operating_income, ordinary_income, net_income,
                       dividend_per_share, forecast_type, revision_direction
                FROM management_forecasts
                WHERE ticker_code = ? AND fiscal_year = ?
                ORDER BY announced_date DESC LIMIT 1
            """,
                [ticker_code, fy],
            )
            fc_row = cursor.fetchone()
            if fc_row:
                result["forecast"] = dict(fc_row)

            # 期初予想
            cursor = conn.execute(
                """
                SELECT revenue, operating_income, ordinary_income, net_income
                FROM management_forecasts
                WHERE ticker_code = ? AND fiscal_year = ? AND forecast_type = 'initial'
                ORDER BY announced_date ASC LIMIT 1
            """,
                [ticker_code, fy],
            )
            init_row = cursor.fetchone()
            if init_row:
                result["initial_forecast"] = dict(init_row)
        except Exception:
            pass

        return result
    finally:
        conn.close()


def get_available_dates() -> list[str]:
    """決算発表が存在する日付の一覧を取得（降順）"""
    conn = get_db()
    try:
        cursor = conn.execute(
            """
            SELECT DISTINCT announcement_date
            FROM financials
            WHERE announcement_date IS NOT NULL
            ORDER BY announcement_date DESC
            LIMIT 60
        """
        )
        return [row["announcement_date"] for row in cursor.fetchall()]
    finally:
        conn.close()


def get_financial_history(ticker_code: str) -> dict:
    """
    業績詳細パネル用データを取得。

    累計実績（最大12四半期）、単独四半期実績（最大12四半期）、会社予想を返す。
    各項目にはYoY%（前年同期比）を付与する。
    """
    conn = get_db()
    try:
        # --- 会社名 ---
        cur = conn.execute(
            "SELECT company_name FROM companies WHERE ticker_code = ?",
            [ticker_code],
        )
        company_row = cur.fetchone()
        company_name = company_row["company_name"] if company_row else ""

        result: dict = {
            "ticker_code": ticker_code,
            "company_name": company_name,
            "cumulative": [],
            "quarterly": [],
            "forecast": None,
        }

        # --- ヘルパー ---
        _q_order = {"Q1": 1, "Q2": 2, "Q3": 3, "Q4": 4}

        def _normalize_quarter(fq: str) -> str:
            """Q4 → FY に正規化（FY/Q4マージ用）"""
            return "FY" if fq in ("FY", "Q4") else fq

        def _q_label_short(fy: str, fq: str) -> str:
            """'24/FY', '25/2Q' 形式の短ラベルを生成"""
            short_year = fy[-2:]
            norm = _normalize_quarter(fq)
            if norm == "FY":
                return f"{short_year}/FY"
            num = _q_order.get(fq, 0)
            return f"{short_year}/{num}Q"

        def _yoy_pct(cur_val, prev_val) -> Optional[float]:
            """YoY%を計算"""
            if cur_val is not None and prev_val is not None and prev_val != 0:
                return round((cur_val - prev_val) * 100.0 / abs(prev_val), 1)
            return None

        # ==================================================
        # 1. 累計データ（Q1-Q4 + FY）を取得し、四半期タイプ別にグループ化
        # ==================================================
        cum_cursor = conn.execute(
            """
            SELECT fiscal_year, fiscal_quarter,
                   revenue, operating_income, ordinary_income, net_income, eps
            FROM financials
            WHERE ticker_code = ?
              AND fiscal_quarter IN ('Q1','Q2','Q3','Q4','FY')
            ORDER BY fiscal_year DESC,
                     CASE fiscal_quarter
                         WHEN 'FY' THEN 5 WHEN 'Q4' THEN 4 WHEN 'Q3' THEN 3
                         WHEN 'Q2' THEN 2 WHEN 'Q1' THEN 1
                     END DESC
            LIMIT 20
            """,
            [ticker_code],
        )
        cum_rows = cum_cursor.fetchall()
        cum_rows = list(reversed(cum_rows))  # oldest first

        # オリジナルキーのマップ（単独四半期EPS計算用に維持）
        cum_map: dict[tuple[str, str], sqlite3.Row] = {}
        for r in cum_rows:
            cum_map[(r["fiscal_year"], r["fiscal_quarter"])] = r

        # 正規化キーのマップ（FY/Q4マージ用。FY優先）
        cum_map_norm: dict[tuple[str, str], sqlite3.Row] = {}
        for r in cum_rows:
            norm_q = _normalize_quarter(r["fiscal_quarter"])
            key = (r["fiscal_year"], norm_q)
            if key not in cum_map_norm or r["fiscal_quarter"] == "FY":
                cum_map_norm[key] = r

        # 最新の四半期タイプを特定し、同タイプの年度推移を表示
        _GROUP_TITLES = {
            "FY": "通期", "Q1": "1Q累計", "Q2": "2Q累計", "Q3": "3Q累計",
        }

        # 最新レコード（cum_rowsは oldest first なので末尾が最新）
        latest_q = _normalize_quarter(cum_rows[-1]["fiscal_quarter"]) if cum_rows else None

        cumulative_list = []
        cumulative_title = ""
        if latest_q:
            cumulative_title = _GROUP_TITLES.get(latest_q, "累計決算")
            # 同タイプの行を抽出（oldest first 維持）
            same_q_rows = [
                r for r in cum_rows
                if _normalize_quarter(r["fiscal_quarter"]) == latest_q
            ]
            # 重複排除（FY/Q4マージ）
            seen_years: set[str] = set()
            unique_rows = []
            for r in reversed(same_q_rows):  # newest first で FY 優先
                if r["fiscal_year"] not in seen_years:
                    seen_years.add(r["fiscal_year"])
                    unique_rows.append(r)
            unique_rows.reverse()  # oldest first に戻す
            display_rows = unique_rows[-3:]

            for r in display_rows:
                fy = r["fiscal_year"]
                fq = r["fiscal_quarter"]
                prev_fy = str(int(fy) - 1)
                prev = cum_map_norm.get((prev_fy, latest_q))

                entry = {
                    "label": _q_label_short(fy, fq),
                    "revenue": r["revenue"],
                    "revenue_yoy_pct": _yoy_pct(
                        r["revenue"], prev["revenue"] if prev else None
                    ),
                    "operating_income": r["operating_income"],
                    "operating_income_yoy_pct": _yoy_pct(
                        r["operating_income"],
                        prev["operating_income"] if prev else None,
                    ),
                    "ordinary_income": r["ordinary_income"],
                    "ordinary_income_yoy_pct": _yoy_pct(
                        r["ordinary_income"],
                        prev["ordinary_income"] if prev else None,
                    ),
                    "net_income": r["net_income"],
                    "net_income_yoy_pct": _yoy_pct(
                        r["net_income"], prev["net_income"] if prev else None
                    ),
                    "eps": r["eps"],
                    "eps_yoy_pct": _yoy_pct(
                        r["eps"], prev["eps"] if prev else None
                    ),
                }
                cumulative_list.append(entry)

        result["cumulative"] = cumulative_list
        result["cumulative_title"] = cumulative_title

        # ==================================================
        # 2. 単独四半期データ（v_financials_standalone_quarter）
        # ==================================================
        sq_cursor = conn.execute(
            """
            SELECT fiscal_year, fiscal_quarter,
                   revenue_standalone, operating_income_standalone,
                   ordinary_income_standalone, net_income_standalone,
                   has_prev_quarter
            FROM v_financials_standalone_quarter
            WHERE ticker_code = ? AND fiscal_quarter IN ('Q1','Q2','Q3','Q4')
            ORDER BY fiscal_year DESC,
                     CASE fiscal_quarter
                         WHEN 'Q4' THEN 4 WHEN 'Q3' THEN 3
                         WHEN 'Q2' THEN 2 WHEN 'Q1' THEN 1
                     END DESC
            LIMIT 16
            """,
            [ticker_code],
        )
        sq_rows = sq_cursor.fetchall()
        sq_rows = list(reversed(sq_rows))  # oldest first

        # 単独四半期 EPS を累計 EPS から計算
        # cum_map を使い: Q1 → そのまま, Q2 → Q2累計 - Q1累計, ...
        def _eps_standalone(fy: str, fq: str) -> Optional[float]:
            """累計EPSから単独四半期EPSを算出"""
            cur_cum = cum_map.get((fy, fq))
            if cur_cum is None or cur_cum["eps"] is None:
                return None
            if fq == "Q1":
                return cur_cum["eps"]
            prev_q_map = {"Q2": "Q1", "Q3": "Q2", "Q4": "Q3"}
            prev_fq = prev_q_map.get(fq)
            if prev_fq is None:
                return None
            prev_cum = cum_map.get((fy, prev_fq))
            if prev_cum is None or prev_cum["eps"] is None:
                return None
            return cur_cum["eps"] - prev_cum["eps"]

        # YoY計算用の辞書
        # has_prev_quarter = 0 の行は登録しない（不正確なstandalone値をYoY計算に使わない）
        sq_map: dict[tuple[str, str], dict] = {}
        for r in sq_rows:
            fy, fq = r["fiscal_year"], r["fiscal_quarter"]
            if not r["has_prev_quarter"]:
                continue  # 前四半期データ欠損の行はYoY計算に使用しない
            sq_map[(fy, fq)] = {
                "revenue_standalone": r["revenue_standalone"],
                "operating_income_standalone": r["operating_income_standalone"],
                "ordinary_income_standalone": r["ordinary_income_standalone"],
                "net_income_standalone": r["net_income_standalone"],
                "eps_standalone": _eps_standalone(fy, fq),
            }

        display_sq = sq_rows[-12:] if len(sq_rows) > 12 else sq_rows

        quarterly_list = []
        for r in display_sq:
            fy, fq = r["fiscal_year"], r["fiscal_quarter"]
            has_prev = r["has_prev_quarter"]

            if not has_prev:
                # 前四半期データ欠損: 単独四半期を正しく計算できないため「-」表示
                entry = {
                    "label": _q_label_short(fy, fq),
                    "revenue": None,
                    "revenue_yoy_pct": None,
                    "operating_income": None,
                    "operating_income_yoy_pct": None,
                    "ordinary_income": None,
                    "ordinary_income_yoy_pct": None,
                    "net_income": None,
                    "net_income_yoy_pct": None,
                    "eps": None,
                    "eps_yoy_pct": None,
                }
                quarterly_list.append(entry)
                continue

            # 以下は既存のロジック（前四半期データありの場合）
            # 前四半期との比較（QoQ）
            prev_q_map = {
                "Q2": ("Q1", fy),
                "Q3": ("Q2", fy),
                "Q4": ("Q3", fy),
                "Q1": ("Q4", str(int(fy) - 1)),
            }
            prev_info = prev_q_map.get(fq)
            cur_data = sq_map.get((fy, fq), {})
            if prev_info:
                prev_fq, prev_fy_q = prev_info
                prev_data = sq_map.get((prev_fy_q, prev_fq), {})
            else:
                prev_data = {}

            eps_sa = cur_data.get("eps_standalone")
            eps_sa_prev = prev_data.get("eps_standalone")

            entry = {
                "label": _q_label_short(fy, fq),
                "revenue": r["revenue_standalone"],
                "revenue_yoy_pct": _yoy_pct(
                    r["revenue_standalone"],
                    prev_data.get("revenue_standalone"),
                ),
                "operating_income": r["operating_income_standalone"],
                "operating_income_yoy_pct": _yoy_pct(
                    r["operating_income_standalone"],
                    prev_data.get("operating_income_standalone"),
                ),
                "ordinary_income": r["ordinary_income_standalone"],
                "ordinary_income_yoy_pct": _yoy_pct(
                    r["ordinary_income_standalone"],
                    prev_data.get("ordinary_income_standalone"),
                ),
                "net_income": r["net_income_standalone"],
                "net_income_yoy_pct": _yoy_pct(
                    r["net_income_standalone"],
                    prev_data.get("net_income_standalone"),
                ),
                "eps": eps_sa,
                "eps_yoy_pct": _yoy_pct(eps_sa, eps_sa_prev),
            }
            quarterly_list.append(entry)

        result["quarterly"] = quarterly_list

        # ==================================================
        # 3. 会社予想（最新FY予想）
        # ==================================================
        try:
            fc_cursor = conn.execute(
                """
                SELECT fiscal_year,
                       revenue, operating_income, ordinary_income, net_income, eps
                FROM management_forecasts
                WHERE ticker_code = ? AND fiscal_quarter = 'FY'
                ORDER BY fiscal_year DESC, announced_date DESC
                LIMIT 1
                """,
                [ticker_code],
            )
            fc_row = fc_cursor.fetchone()
            if fc_row:
                fc_fy = fc_row["fiscal_year"]
                # 前年度FY実績との比較
                fy_actual_cursor = conn.execute(
                    """
                    SELECT revenue, operating_income, ordinary_income,
                           net_income, eps
                    FROM financials
                    WHERE ticker_code = ? AND fiscal_year = ?
                          AND fiscal_quarter = 'FY'
                    LIMIT 1
                    """,
                    [ticker_code, str(int(fc_fy) - 1)],
                )
                fy_actual = fy_actual_cursor.fetchone()
                # FYが無ければQ4累計を代用
                if not fy_actual:
                    fy_actual_cursor = conn.execute(
                        """
                        SELECT revenue, operating_income, ordinary_income,
                               net_income, eps
                        FROM financials
                        WHERE ticker_code = ? AND fiscal_year = ?
                              AND fiscal_quarter = 'Q4'
                        LIMIT 1
                        """,
                        [ticker_code, str(int(fc_fy) - 1)],
                    )
                    fy_actual = fy_actual_cursor.fetchone()

                result["forecast"] = {
                    "fiscal_year": fc_fy,
                    "revenue": fc_row["revenue"],
                    "revenue_yoy_pct": _yoy_pct(
                        fc_row["revenue"],
                        fy_actual["revenue"] if fy_actual else None,
                    ),
                    "operating_income": fc_row["operating_income"],
                    "operating_income_yoy_pct": _yoy_pct(
                        fc_row["operating_income"],
                        fy_actual["operating_income"] if fy_actual else None,
                    ),
                    "ordinary_income": fc_row["ordinary_income"],
                    "ordinary_income_yoy_pct": _yoy_pct(
                        fc_row["ordinary_income"],
                        fy_actual["ordinary_income"] if fy_actual else None,
                    ),
                    "net_income": fc_row["net_income"],
                    "net_income_yoy_pct": _yoy_pct(
                        fc_row["net_income"],
                        fy_actual["net_income"] if fy_actual else None,
                    ),
                    "eps": fc_row["eps"],
                    "eps_yoy_pct": _yoy_pct(
                        fc_row["eps"],
                        fy_actual["eps"] if fy_actual else None,
                    ),
                }
        except Exception:
            pass  # management_forecasts テーブル未作成時は無視

        return result
    finally:
        conn.close()


def _calc_changes(current_row, prev_row) -> dict:
    """当期と前期の変化率を計算"""
    fields = [
        "revenue",
        "gross_profit",
        "operating_income",
        "ordinary_income",
        "net_income",
    ]
    result = {}
    for f in fields:
        cur = current_row[f] if current_row else None
        prev = prev_row[f] if prev_row else None
        result[f] = cur
        result[f"{f}_prev"] = prev
        if cur is not None and prev is not None and prev != 0:
            result[f"{f}_pct"] = round((cur - prev) * 100.0 / abs(prev), 1)
        else:
            result[f"{f}_pct"] = None
    return result
