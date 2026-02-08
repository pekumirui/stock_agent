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
| `OrdinaryIncomeINS` | 保険業（経常収益） |
| `OperatingIncomeINS` | 保険業（営業収益）※名前は紛らわしいが売上相当 |
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

### IFRS有報サマリー (jpcrp_cor)

IFRS採用企業の有報・半期報では、`*IFRSSummaryOfBusinessResults` サフィックス付き要素が使われる。
**注意**: `jpcrp_cor` 名前空間に属するため、`XBRL_FACT_MAPPING`（日本基準側）に定義する。

| 要素名 | マッピング先 |
|---|---|
| `RevenueIFRSSummaryOfBusinessResults` | revenue |
| `OperatingProfitLossIFRSSummaryOfBusinessResults` | operating_income |
| `ProfitLossBeforeTaxIFRSSummaryOfBusinessResults` | ordinary_income（税引前利益） |
| `ProfitLossAttributableToOwnersOfParentIFRSSummaryOfBusinessResults` | net_income |
| `BasicEarningsLossPerShareIFRSSummaryOfBusinessResults` | eps |
| `DilutedEarningsLossPerShareIFRSSummaryOfBusinessResults` | eps |

### US-GAAP有報サマリー (jpcrp_cor)

US-GAAP採用企業（オムロン、野村HD、富士フイルム等）の有報・半期報では、`*USGAAPSummaryOfBusinessResults` サフィックス付き要素が使われる。
**注意**: `jpcrp_cor` 名前空間に属するため、`XBRL_FACT_MAPPING`（日本基準側）に定義する。

| 要素名 | マッピング先 |
|---|---|
| `RevenuesUSGAAPSummaryOfBusinessResults` | revenue |
| `ProfitLossBeforeTaxUSGAAPSummaryOfBusinessResults` | ordinary_income（税引前利益） |
| `NetIncomeLossAttributableToOwnersOfParentUSGAAPSummaryOfBusinessResults` | net_income |
| `BasicEarningsLossPerShareUSGAAPSummaryOfBusinessResults` | eps |
| `DilutedEarningsLossPerShareUSGAAPSummaryOfBusinessResults` | eps |

**制限**: US-GAAPサマリーには gross_profit, operating_income の要素が含まれない。
半期報告書のXBRLにはUS-GAAP P/L名前空間（`jpus_cor`等）が含まれないため、これらは取得不可。

## 売上総利益の要素名

### 日本基準 (jppfs_cor)
| 要素名 | 業種 |
|---|---|
| `GrossProfit` | 一般企業（売上総利益） |
| `GrossProfitOnCompletedConstructionContracts` | 建設業（完成工事総利益） |
| `GrossProfitOnCompletedConstructionContractsCNS` | 建設業（連結） |
| `NetOperatingRevenueSEC` | 第一種金融商品取引業（純営業収益） |
| `OperatingGrossProfit` | 一般商工業（営業総利益） |
| `OperatingGrossProfitWAT` | 海運業（営業総利益） |

### IFRS
| 要素名 | 説明 |
|---|---|
| `GrossProfit` | IFRS標準（日本基準と共通） |
| `GrossProfitIFRS` | jpigp_cor用 |

## マッピング追加の手順

1. `[DEBUG] 未マッチXBRL要素` ログで未対応の要素名を特定
2. 要素名のタクソノミ（jppfs_cor / ifrs-full等）を確認
3. `XBRL_FACT_MAPPING`（日本基準）または `XBRL_FACT_MAPPING_IFRS`（IFRS）に追加
4. `jpcrp_cor` 名前空間の要素は `XBRL_FACT_MAPPING` 側に追加すること（JPPFS_NAMESPACE_PATTERNSに含まれるため）
5. IFRS Summary要素（`*IFRSSummaryOfBusinessResults`）も `jpcrp_cor` なので `XBRL_FACT_MAPPING` 側に追加
6. US-GAAP Summary要素（`*USGAAPSummaryOfBusinessResults`）も同様に `XBRL_FACT_MAPPING` 側に追加
7. `tests/test_fetch_financials.py` にテスト追加

## 注意事項

- IFRSには「経常利益」がない → `ProfitLossBeforeTax`（税引前利益）を `ordinary_income` にマッピング
- 検索順序: `XBRL_FACT_MAPPING` → `XBRL_FACT_MAPPING_IFRS`（最初にマッチした値を優先）
- コンテキスト判定: `CurrentYearDuration` / `InterimPeriodDuration` 等が当期データ、`Prior*` は前期

## 構造的欠損（修正不要）

以下のケースは業種・P/L構造上、該当フィールドが存在しないため None が正常:

| 業種/パターン | 欠損フィールド | 理由 |
|---|---|---|
| 銀行業 | gross_profit, operating_income | 銀行P/Lには売上原価・営業利益の概念がない |
| 保険業 | gross_profit, operating_income | 保険P/Lは経常収益→経常利益の構造 |
| 一部FG・持株会社 | gross_profit | 連結特有の勘定科目構成 |
| プレ売上バイオ等 | revenue, gross_profit | 開発段階で売上なし |
| US-GAAP企業（半期報） | gross_profit, operating_income | サマリーに要素なし、P/L名前空間未タグ付け |
| IT/サービス企業 | gross_profit | 販管費一括表示P/L（売上原価の概念なし） |
| 広告代理店等 | revenue | 純額表示（2021年収益認識基準適用後） |
| 一部IFRS企業（半期報） | gross_profit | IFRS P/Lで Revenue→Operating Profit の簡略フォーマット |
| 商社等(IFRS) | operating_income | IFRSでは営業利益の開示が任意 |
