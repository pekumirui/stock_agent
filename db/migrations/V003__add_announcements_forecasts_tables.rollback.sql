-- V003 Rollback: add_announcements_forecasts_tables
--
-- ロールバック処理
--

DROP VIEW IF EXISTS v_financials_standalone_quarter;
DROP TABLE IF EXISTS consensus_estimates;
DROP TABLE IF EXISTS management_forecasts;
DROP TABLE IF EXISTS announcements;
-- Note: SQLite does not support DROP COLUMN.
-- announcement_time column on financials cannot be removed without recreating the table.
