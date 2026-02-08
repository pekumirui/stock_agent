"""
test_update_edinet_codes.py - EDINETコード更新スクリプトのテスト

このテストでは、update_edinet_codes.py の以下の機能をテストします:
- parse_sec_code: 5桁→4桁証券コード変換
- get_companies_without_edinet: 未登録銘柄の取得
- update_edinet_codes: EDINETコードの更新ロジック

テスト方針:
- 実API統合テストは含まず（fetch_edinet_codelist は手動で確認）
- DB操作とフィルタリングロジックを重点的にテスト
"""
import pytest
from update_edinet_codes import (
    parse_sec_code,
    get_companies_without_edinet,
    update_edinet_codes,
)
from db_utils import get_connection, upsert_company


class TestParseSecCode:
    """証券コード変換のテスト"""

    def test_valid_5digit_code(self):
        """5桁の証券コードが4桁に変換されること"""
        assert parse_sec_code("67580") == "6758"
        assert parse_sec_code("72030") == "7203"

    def test_valid_4digit_code(self):
        """4桁の証券コードがそのまま返されること"""
        assert parse_sec_code("6758") == "6758"
        assert parse_sec_code("7203") == "7203"

    def test_none_input(self):
        """Noneが入力されたらNoneを返すこと"""
        assert parse_sec_code(None) is None

    def test_empty_string(self):
        """空文字が入力されたらNoneを返すこと"""
        assert parse_sec_code("") is None

    def test_short_code(self):
        """3桁以下の証券コードはNoneを返すこと"""
        assert parse_sec_code("123") is None
        assert parse_sec_code("12") is None

    def test_5digit_with_letter_stays_unchanged(self):
        """英字付き5桁コードはそのまま（変換しない）"""
        assert parse_sec_code("2914A") == "2914A"  # 日本たばこ産業 優先株
        assert parse_sec_code("1234B") == "1234B"

    def test_5digit_not_ending_with_zero(self):
        """末尾0以外の5桁コードはそのまま（変換しない）"""
        # is_valid_ticker_code() で有効と判定される5桁数字コード
        # 実際にはこのようなコードは稀だが、テストとして確認
        assert parse_sec_code("12345") == "12345"
        assert parse_sec_code("98761") == "98761"

    def test_whitespace_trimming(self):
        """前後の空白は削除される"""
        assert parse_sec_code("  7974  ") == "7974"
        assert parse_sec_code("  79740  ") == "7974"
        assert parse_sec_code("\t7203\n") == "7203"

    def test_real_world_edinet_codes(self):
        """実際のEDINET APIから返されるコードのテスト（2025-11-07データ）"""
        # EDINET APIが実際に返す5桁形式（末尾0）
        assert parse_sec_code("79740") == "7974"  # 任天堂
        assert parse_sec_code("68770") == "6877"  # OBARA GROUP
        assert parse_sec_code("45920") == "4592"  # サンバイオ
        assert parse_sec_code("63250") == "6325"  # タカキタ
        assert parse_sec_code("81500") == "8150"  # 三信電気
        assert parse_sec_code("61400") == "6140"  # 旭ダイヤモンド工業


class TestGetCompaniesWithoutEdinet:
    """EDINETコード未登録銘柄取得のテスト"""

    def test_all_companies_have_edinet(self, test_db):
        """すべての銘柄がEDINETコード登録済みの場合"""
        # EDINETコード付きで銘柄を登録
        upsert_company("9999", "テスト株式会社", edinet_code="E99999")
        upsert_company("9998", "テスト株式会社2", edinet_code="E99998")

        result = get_companies_without_edinet()

        # テスト用銘柄（9xxx番台）のみをチェック
        test_tickers = {t for t in result if t.startswith('9')}
        assert test_tickers == set()

    def test_some_companies_without_edinet(self, test_db):
        """一部の銘柄がEDINETコード未登録の場合"""
        # EDINETコード付き
        upsert_company("9999", "テスト株式会社", edinet_code="E99999")
        # EDINETコードなし
        upsert_company("9998", "テスト株式会社2", edinet_code=None)
        upsert_company("9997", "テスト株式会社3", edinet_code=None)

        result = get_companies_without_edinet()

        # テスト用銘柄（9xxx番台）のみをチェック
        test_tickers = {t for t in result if t.startswith('9')}
        assert test_tickers == {"9998", "9997"}

    def test_all_companies_without_edinet(self, test_db):
        """すべての銘柄がEDINETコード未登録の場合"""
        upsert_company("9999", "テスト株式会社", edinet_code=None)
        upsert_company("9998", "テスト株式会社2", edinet_code=None)

        result = get_companies_without_edinet()

        # テスト用銘柄（9xxx番台）のみをチェック
        test_tickers = {t for t in result if t.startswith('9')}
        assert test_tickers == {"9999", "9998"}

    def test_empty_database(self, test_db):
        """銘柄がまったく登録されていない場合"""
        result = get_companies_without_edinet()
        # テスト用銘柄（9xxx番台）のみをチェック
        test_tickers = {t for t in result if t.startswith('9')}
        assert test_tickers == set()


class TestUpdateEdinetCodes:
    """EDINETコード更新のテスト"""

    def test_all_registered_early_return(self, test_db):
        """すべて登録済みの場合は早期リターンすること"""
        # EDINETコード付きで銘柄を登録
        upsert_company("9999", "テスト株式会社", edinet_code="E99999")

        # APIデータ（同じ銘柄）
        api_data = {
            'data': [
                {
                    'edinetCode': 'E99999',
                    'secCode': '99990',
                    'filerName': 'テスト株式会社'
                }
            ]
        }

        updated, matched = update_edinet_codes(api_data)

        # すべて登録済みなので更新なし
        assert updated == 0
        assert matched == 0

    def test_updates_missing_only(self, test_db):
        """未登録銘柄のみ更新されること"""
        # EDINETコードなしで銘柄を登録
        upsert_company("9999", "テスト株式会社", edinet_code=None)
        upsert_company("9998", "テスト株式会社2", edinet_code=None)

        # APIデータ
        api_data = {
            'data': [
                {
                    'edinetCode': 'E99999',
                    'secCode': '99990',
                    'filerName': 'テスト株式会社'
                },
                {
                    'edinetCode': 'E99998',
                    'secCode': '99980',
                    'filerName': 'テスト株式会社2'
                }
            ]
        }

        updated, matched = update_edinet_codes(api_data)

        # 2銘柄が更新されること
        assert updated == 2
        assert matched == 2

        # DBを確認
        with get_connection() as conn:
            cursor = conn.execute(
                "SELECT ticker_code, edinet_code FROM companies WHERE ticker_code IN ('9999', '9998') ORDER BY ticker_code"
            )
            rows = cursor.fetchall()
            assert len(rows) == 2
            assert rows[0]['ticker_code'] == '9998'
            assert rows[0]['edinet_code'] == 'E99998'
            assert rows[1]['ticker_code'] == '9999'
            assert rows[1]['edinet_code'] == 'E99999'

    def test_skip_registered_companies(self, test_db):
        """登録済み銘柄はスキップされること"""
        # 1つは登録済み、1つは未登録
        upsert_company("9999", "テスト株式会社", edinet_code="E99999")
        upsert_company("9998", "テスト株式会社2", edinet_code=None)

        # APIデータ（両方とも含む）
        api_data = {
            'data': [
                {
                    'edinetCode': 'E99999',
                    'secCode': '99990',
                    'filerName': 'テスト株式会社'
                },
                {
                    'edinetCode': 'E99998',
                    'secCode': '99980',
                    'filerName': 'テスト株式会社2'
                }
            ]
        }

        updated, matched = update_edinet_codes(api_data)

        # 未登録の9998のみ更新されること
        assert updated == 1
        assert matched == 1

        # DBを確認（9999は変わらず、9998が更新）
        with get_connection() as conn:
            cursor = conn.execute(
                "SELECT ticker_code, edinet_code FROM companies WHERE ticker_code = '9999'"
            )
            row = cursor.fetchone()
            assert row['edinet_code'] == 'E99999'

            cursor = conn.execute(
                "SELECT ticker_code, edinet_code FROM companies WHERE ticker_code = '9998'"
            )
            row = cursor.fetchone()
            assert row['edinet_code'] == 'E99998'

    def test_duplicate_handling(self, test_db):
        """重複データがあっても正しく処理されること"""
        upsert_company("9999", "テスト株式会社", edinet_code=None)

        # 同じ銘柄のデータが2回含まれる
        api_data = {
            'data': [
                {
                    'edinetCode': 'E99999',
                    'secCode': '99990',
                    'filerName': 'テスト株式会社'
                },
                {
                    'edinetCode': 'E99999',
                    'secCode': '99990',
                    'filerName': 'テスト株式会社'
                }
            ]
        }

        updated, matched = update_edinet_codes(api_data)

        # 1回だけ更新されること（重複は2回目でスキップ）
        assert updated == 1
        assert matched == 1

        # DBを確認
        with get_connection() as conn:
            cursor = conn.execute(
                "SELECT ticker_code, edinet_code FROM companies WHERE ticker_code = '9999'"
            )
            row = cursor.fetchone()
            assert row['edinet_code'] == 'E99999'

    def test_skip_invalid_sec_code(self, test_db):
        """無効な証券コードはスキップされること"""
        upsert_company("9999", "テスト株式会社", edinet_code=None)

        # 無効な証券コード
        api_data = {
            'data': [
                {
                    'edinetCode': 'E99999',
                    'secCode': '',  # 空文字
                    'filerName': 'テスト株式会社'
                },
                {
                    'edinetCode': 'E99998',
                    'secCode': None,  # None
                    'filerName': 'テスト株式会社2'
                }
            ]
        }

        updated, matched = update_edinet_codes(api_data)

        # どちらもスキップされる
        assert updated == 0
        assert matched == 0

    def test_skip_nonexistent_ticker(self, test_db):
        """DBに存在しない銘柄はスキップされること"""
        # 9999のみ登録、9998は未登録
        upsert_company("9999", "テスト株式会社", edinet_code=None)

        # APIデータには9998も含まれる
        api_data = {
            'data': [
                {
                    'edinetCode': 'E99999',
                    'secCode': '99990',
                    'filerName': 'テスト株式会社'
                },
                {
                    'edinetCode': 'E99998',
                    'secCode': '99980',
                    'filerName': 'テスト株式会社2'
                }
            ]
        }

        updated, matched = update_edinet_codes(api_data)

        # 9999のみ更新、9998はスキップ
        assert updated == 1
        assert matched == 1

    def test_empty_data(self, test_db):
        """空のデータが渡された場合"""
        api_data = {'data': []}

        updated, matched = update_edinet_codes(api_data)

        # 更新なし
        assert updated == 0
        assert matched == 0

    def test_invalid_data_structure(self, test_db):
        """不正なデータ構造の場合"""
        api_data = None

        updated, matched = update_edinet_codes(api_data)

        # エラーハンドリングで0を返す
        assert updated == 0
        assert matched == 0
