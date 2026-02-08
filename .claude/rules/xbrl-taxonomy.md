# XBRLタクソノミ - 要素名マッピングガイド

## 概要

EDINET/TDnetのXBRL財務データパースでは、会計基準・業種ごとに異なる要素名(local_name)が使われる。
マッピング定義: `scripts/fetch_financials.py` の `XBRL_FACT_MAPPING`（日本基準）/ `XBRL_FACT_MAPPING_IFRS`（IFRS）

## 名前空間パターン

### 日本基準（JPPFS_NAMESPACE_PATTERNS）
- `jppfs_cor` - 財務諸表本表の要素（NetSales, GrossProfit等）
- `jpcrp_cor` - 企業内容等開示タクソノミ（有報の経営指標 `*SummaryOfBusinessResults`、EPS等）

### IFRS（IFRS_NAMESPACE_PATTERNS）
- `ifrs-full` / `ifrs_cor` - IFRS本体タクソノミ
- `jpcif_cor` - 日本IFRS統合タクソノミ
- `jpigp_cor` - 日本IFRS汎用タクソノミ（TDnet Attachment等）
- `tse-ed-t` - 東証電子開示タクソノミ（TDnet Summary）

## 売上高の業種別要素名

### 日本基準 (jppfs_cor)
| 要素名 | 業種 |
|---|---|
| `NetSales` | 一般企業（製造・小売等） |
| `OperatingRevenue` | 汎用的な営業収益 |
| `OperatingRevenue1` | 鉄道・バス・不動産・通信 |
| `OperatingRevenue2` | 保険業 |
| `NetSalesOfCompletedConstructionContracts` | 建設業（完成工事高） |
| `NetSalesAndOperatingRevenue` | 電力・ガス等 |
| `BusinessRevenue` | 商社・サービス |
| `OperatingRevenueELE` | 電力業 |
| `ShippingBusinessRevenueWAT` | 海運業 |
| `OperatingRevenueSEC` | 証券業 |
| `OperatingRevenueSPF` | 特定金融業 |
| `OrdinaryIncomeBNK` | 銀行業（経常収益） |
| `TotalOperatingRevenue` | 営業収益合計 |

### IFRS
| 要素名 | 説明 |
|---|---|
| `Revenue` | 標準IFRS（日本基準と共通） |
| `RevenueFromContractsWithCustomers` | IFRS 15 準拠 |
| `SalesIFRS` | TDnet用 |
| `RevenueIFRS` | EDINET IFRS対応 |
| `OperatingRevenueIFRS` | IFRS営業収益 |

### 有報 経営指標サマリー (jpcrp_cor)
P/L本表とは別に、有報の「経営指標等の推移」セクションにも売上高が記載される。
要素名は `*SummaryOfBusinessResults` サフィックス付き（例: `NetSalesSummaryOfBusinessResults`）。

## 売上総利益の要素名

| 要素名 | 会計基準 |
|---|---|
| `GrossProfit` | 日本基準・IFRS共通 |
| `GrossProfitIFRS` | jpigp_cor用 |
| `GrossProfitOnCompletedConstructionContracts` | 建設業（完成工事総利益） |

## マッピング追加の手順

1. `[DEBUG] 未マッチXBRL要素` ログで未対応の要素名を特定
2. 要素名のタクソノミ（jppfs_cor / ifrs-full等）を確認
3. `XBRL_FACT_MAPPING`（日本基準）または `XBRL_FACT_MAPPING_IFRS`（IFRS）に追加
4. `jpcrp_cor` 名前空間の要素は `XBRL_FACT_MAPPING` 側に追加すること（JPPFS_NAMESPACE_PATTERNSに含まれるため）
5. `tests/test_fetch_financials.py` にテスト追加

## 注意事項

- IFRSには「経常利益」がない → `ProfitLossBeforeTax`（税引前利益）を `ordinary_income` にマッピング
- 検索順序: `XBRL_FACT_MAPPING` → `XBRL_FACT_MAPPING_IFRS`（最初にマッチした値を優先）
- コンテキスト判定: `CurrentYearDuration` / `InterimPeriodDuration` 等が当期データ、`Prior*` は前期
