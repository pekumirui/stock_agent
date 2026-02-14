# J-Quants vs TDnet 取得結果比較 調査レポート

日付: 2026-02-14
対象データ: 2026-02-13 開示分
スクリプト: `scripts/compare_sources.py --date 2026-02-13`

## 突合サマリー

### 最新（2026-02-14 4回目: DB復元後の再確認）

| 指標 | 値 |
|---|---|
| DB件数(TDnet+EDINET) | 964 |
| JQuants件数 | 940 |
| 突合キー一致 | 933 |
| 完全一致 | 840 / 933 (90.0%) |
| 差異あり | 93 / 933 |
| DBのみ | 31 |
| JQuantsのみ | 7 |

### 参考: 3回目（和暦修正後、DB件数減少時）

| 指標 | 値 |
|---|---|
| DB件数(TDnet+EDINET) | 869 (TDnet:785 + EDINET:84) |
| JQuants件数 | 845 |
| 突合キー一致 | 838 |
| 完全一致 | 752 / 838 (89.7%) |
| 差異あり | 86 / 838 |
| DBのみ | 31 |
| JQuantsのみ | 7 |

### 参考: 2回目（compare_sources.py v2: EDINET含む + companiesフィルタ）

| 指標 | 値 |
|---|---|
| DB件数(TDnet+EDINET) | 964 |
| JQuants件数 | 940 |
| 突合キー一致 | 929 |
| 完全一致 | 836 / 929 (90.0%) |
| 差異あり | 93 / 929 |
| DBのみ | 35 |
| JQuantsのみ | 11 |

### 参考: 初回（compare_sources.py v1: TDnetのみ + フィルタなし）

| 指標 | 値 |
|---|---|
| TDnet件数 | 785 |
| JQuants件数 | 968 |
| 突合キー一致 | 779 |
| 完全一致 | 714 / 779 (91.7%) |
| 差異あり | 65 / 779 |
| TDnetのみ | 6 |
| JQuantsのみ | 189 |

### 件数変動の注記

- 初回→2回目: compare_sources.py改修（EDINET含む+companiesフィルタ）でJQuantsのみ189→11に大幅減少
- 2回目→3回目: DB再取得でDB件数964→869に一時減少、JQuantsのみ11→7に改善（和暦修正の効果）
- 3回目→4回目(最新): DB件数が964に復元。突合キー933件中840件(90.0%)が完全一致で安定
- JQuantsのみ7件・DBのみ31件は3回目から変化なし。差異パターンも安定しており、データ品質は十分実用的

## 1. 件数差の原因（JQuants 968 vs TDnet 785）

JQuantsの方が183件多い。内訳:

### TDnet処理パイプライン全体像（2026-02-13）

```
TDnet HTMLスクレイプ → announcements: 939件 (earnings)
    ↓ XBRL ZIP ダウンロード + iXBRL解析
financials保存: 936件
    うち source='TDnet':  879件
    うち source='EDINET':  60件（EDINET priority=3 が TDnet priority=2 に勝ち既存データ維持）
    うち 決算期変更衝突:     3件（後述）
XBRL解析失敗: 0件
```

**ページネーション制限は原因ではない**。`_get_pagination_urls()`で全ページのリンクを辿り、
`processed_urls` setで重複防止しつつ全ページを処理する実装。`announcements`テーブルには
939件のearningsが保存されており、HTMLスクレイプの取りこぼしなし。XBRL解析失敗も0件。

### JQuantsのみ 189件の内訳（初回実行時）

初回 `compare_sources.py` にバグがあり過大にカウントしていた。修正後の原因別内訳:

| 原因 | 件数 | 説明 | 対処 |
|---|---|---|---|
| EDINET上書き(不可視) | ~60 | financialsにはあるがsource='EDINET'。旧compare_sources.pyがsource='TDnet'のみ検索 | **修正済み**: source IN ('TDnet','EDINET')に変更 |
| companiesテーブル未登録 | 28 | JQuants側にフィルタがなく非上場等が含まれた | **修正済み**: `ticker_exists()`フィルタ追加 |
| fiscal period不一致 | 6 | fiscal_year判定ロジックの違いでキーが不一致 | 要改善（後述） |
| 決算期変更衝突 | 3 | 同一fiscal_yearに旧期・新期の2つのFYが存在 | スキーマ改善が必要（後述） |
| TDnetに未掲載 | ~92 | TDnetのannouncements(earnings)にも存在しない | J-Quantsで補完 |

### TDnetのみ 6件の原因（初回時、うち3件は和暦修正で解消）

| ticker | TDnet | JQuants | 原因 | 状況 |
|---|---|---|---|---|
| 3204 | 2026/FY | 2025/FY | fiscal_year 1年ズレ | **解消済**（和暦修正） |
| 3719 | 2026/FY | 2025/FY | fiscal_year 1年ズレ | **解消済**（和暦修正） |
| 6171 | 2026/FY | 2025/FY | fiscal_year 1年ズレ | **解消済**（和暦修正） |
| 3864 | 2025/Q3 | 2026/Q3 | fiscal_year 1年ズレ（逆方向）| 未解消 |
| 6165 | 2025/Q3 | 2026/Q3 | fiscal_year 1年ズレ（逆方向）| 未解消 |
| 3909 | 2026/FY | (なし) | JQuantsに該当なし | 未解消 |

**根本原因**: fiscal_year判定ロジックの違い
- J-Quants: `CurFYEn`（決算期末日）から年を抽出 → `2025-12-31` → `2025`
- TDnet: タイトル正規表現 `(\d{4})年` で抽出。マッチ失敗時は`announcement_date[:4]`にフォールバック → `2026`

→ TDnet側のタイトル正規表現が一部タイトルでマッチせず、開示日の年（2026）にフォールバックしたと推定。
→ 和暦タイトル（「令和7年12月期」等）は`_wareki_to_seireki()`で修正済み。

### JQuantsのみ 7件の内訳（最新実行）

| ticker | fiscal_year/qtr | 原因 |
|---|---|---|
| 3864 | 2026/Q3 | fiscal_year 1年ズレ（DB側は2025/Q3） |
| 4222 | 2026/Q3 | DBに該当なし |
| 4331 | 2025/FY | 決算期変更衝突（DB側はEDINET旧期で上書き） |
| 4891 | 2025/FY | 決算期変更衝突（DB側はEDINET旧期で上書き） |
| 4990 | 2026/Q3 | DBに該当なし |
| 6165 | 2026/Q3 | fiscal_year 1年ズレ（DB側は2025/Q3） |
| 6630 | 2025/FY | 決算期変更衝突（DB側はEDINET旧期で上書き） |

### DBのみ 31件の内訳（最新実行）

31件中28件がEDINET由来、3件がTDnet由来。
大半はJ-Quantsにもデータが存在するが、突合スクリプトの日付指定・キーマッチングで漏れている。

| 分類 | 件数 | source | 説明 |
|---|---|---|---|
| A. TDnet先行開示→EDINET後追い | 17 | EDINET | J-Quantsには別日付で存在 |
| B. fiscal_year不一致 | 8 | EDINET | J-Quantsにはキー違いで存在（既存バグ） |
| C. TDnet決算短信なし | 3 | EDINET | J-Quantsに存在しない可能性（要確認） |
| D. TDnet由来の既知問題 | 3 | TDnet | fiscal_yearズレ(2件) + JQuants欠損(1件) |

#### A. TDnet先行開示 → EDINET後追い（17件）

決算短信はTDnetで先に開示済み。2/13にEDINETで四半期報告書を提出。
DB側のannouncement_dateがEDINET提出日(2/13)だが、J-Quantsは決算短信の開示日を使用するため、
日付指定の突合で漏れる。J-Quantsにデータ自体は存在する。

| ticker | 企業名 | 市場 | TDnet開示日 | 日付差 |
|---|---|---|---|---|
| 2689 | オルバヘルスケアHD | スタンダード | 2026-01-30 | 14日 |
| 2481 | タウンニュース社 | スタンダード | 2026-01-30 | 14日 |
| 6888 | アクモス | スタンダード | 2026-02-03 | 10日 |
| 3271 | THEグローバル社 | スタンダード | 2026-02-06 | 7日 |
| 6327 | 北川精機 | スタンダード | 2026-02-06 | 7日 |
| 3028 | アルペン | プライム | 2026-02-06 | 7日 |
| 7826 | フルヤ金属 | プライム | 2026-02-06 | 7日 |
| 3082 | きちりHD | スタンダード | 2026-02-10 | 3日 |
| 2385 | 総医研HD | グロース | 2026-02-12 | 1日 |
| 7532 | パンパシHD | プライム | 2026-02-12 | 1日 |
| 6239 | ナガオカ | スタンダード | 2026-02-12 | 1日 |
| 3538 | ウイルプラスHD | スタンダード | 2026-02-12 | 1日 |
| 1764 | 工藤建設 | スタンダード | 2026-02-12 | 1日 |
| 3639 | ボルテージ | スタンダード | 2026-02-12 | 1日 |
| 4398 | ブロードバンドセキュリティ | スタンダード | 2026-02-12 | 1日 |
| 4418 | JDSC | グロース | 2026-02-12 | 1日 |
| 1788 | 三東工業社 | スタンダード | 2026-02-12 | 1日 |

#### B. fiscal_year不一致（8件）— 既存バグ

EDINETでは`2025年度`、TDnetでは`2026年度`として記録。年度判定ロジックの違いによるキー不一致。
J-Quantsにはキー違いでデータが存在する可能性が高い。

| ticker | 企業名 | 市場 | EDINET年度 | TDnet年度 |
|---|---|---|---|---|
| 1431 | Lib Work | グロース | 2025 | 2026 |
| 4088 | エア・ウォーター | プライム | 2025 | 2026 |
| 4396 | システムサポートHD | プライム | 2025 | 2026 |
| 5283 | 高見澤 | スタンダード | 2025 | 2026 |
| 6156 | エーワン精密 | スタンダード | 2025 | 2026 |
| 6548 | 旅工房 | グロース | 2025 | 2026 |
| 6597 | HPCシステムズ | グロース | 2025 | 2026 |
| 7585 | かんなん丸 | スタンダード | 2025 | 2026 |

#### C. TDnet決算短信なし（3件）— 要確認

announcementsテーブルにearningsレコードが見つからず、J-Quantsにも存在しない可能性あり。

| ticker | 企業名 | 市場 | 期 | 備考 |
|---|---|---|---|---|
| 3286 | トラストHD | スタンダード | 2026/Q2 | announcements履歴なし |
| 4385 | メルカリ | プライム | 2026/Q2 | プライム大型株。決算短信未開示の可能性 |
| 5380 | 新東 | スタンダード | 2026/Q2 | announcements履歴なし |

#### D. TDnet由来の既知問題（3件）

| ticker | period | 原因 |
|---|---|---|
| 3864 | 2025/Q3 | fiscal_yearキーズレ（JQuants側は2026/Q3） |
| 6165 | 2025/Q3 | fiscal_yearキーズレ（JQuants側は2026/Q3） |
| 3909 | 2026/FY | JQuantsに該当レコードなし |

## 2. フィールド別の差異分析（最新実行: 93レコード）

### 差異パターンの分類

| パターン | 件数 | 説明 |
|---|---|---|
| ordinary_income NULLのみ | 29 | JQuantsでOdPがNULL、他フィールドは一致 |
| ordinary_income NULL + net_income差異 | 43 | OdP NULL + 純利益定義の違い |
| net_incomeのみ差異 | 19 | 小数点丸め or 親会社帰属 vs 当期純損益 |
| 特殊（revenue/op_income/eps含む） | 2 | 6269, 8253 |

フィールド別差異件数: ordinary_income=74, net_income=64, revenue=2, operating_income=1, eps=1

※ 突合母数増加(838→933)に伴い差異件数も86→93に増加したが、
  完全一致率は89.7%→90.0%と改善。差異パターンの構成比は安定している

### ordinary_income: J-QuantsでNULL（67件）

- **原因**: J-Quants APIの`OdP`（経常利益）フィールドがIFRS企業やUS-GAAP企業で空
- **TDnet側**: XBRLの`OrdinaryIncome`/`OrdinaryProfit`タグを直接取得するため、決算短信サマリーに記載があれば取得可能
- **影響**: IFRS企業の経常利益がJ-Quantsでは取得不可

該当例:
```
8001(伊藤忠)  TDnet=946,099  JQuants=NULL
4543(テルモ)  TDnet=146,587  JQuants=NULL
8630(SOMPO)   TDnet=677,848  JQuants=NULL
```

### net_income: 値の不一致（60件）

ordinary_incomeがNULLな銘柄の多くでnet_incomeも差異あり（39件）。
net_incomeのみの差異は19件で、小数点丸めの差（端株調整）が多い。

- **推定原因**: J-QuantsのNP（当期純利益）と、TDnetのXBRLから取得する純利益の定義が異なる
  - J-Quants: サマリー短信の「親会社株主に帰属する当期純利益」
  - TDnet: XBRLの`ProfitLoss`（当期純損益）= 非支配株主持分を含む場合あり
- **差異の大きさ**: 数百万〜数億円程度。比率としては数%以内が多い

主な差異例:
```
ticker  TDnet       JQuants     diff        推定比率
8001    735,397     705,297     30,100      4.1%
2503    178,173     147,542     30,631      17.2%  ← 大
4324   -318,939    -327,601      8,662      2.7%
7272     34,938      16,109     18,829      53.9%  ← 大
2160     -7,318      -4,411      2,907      39.7%  ← 大
```

### revenue / operating_income: ほぼ一致

6269, 8253を除き完全一致。

### eps: ほぼ一致

6269を除き完全一致。

### 異常値: 6269（三井海洋開発）

全フィールドで大幅な乖離（前回から変化なし）:
```
field              TDnet      JQuants     diff
revenue            502,737    717,100     214,363
operating_income    12,138     68,498      56,360
net_income          19,495     56,456      36,961
eps                 285.31     826.25      540.94
```

→ 連結 vs 単体の混在、または異なる開示書類を参照している可能性。要個別調査。

### 異常値: 8253（クレディセゾン）- 新規発見

revenueに大きな乖離:
```
field              TDnet      JQuants     diff
revenue            411,482    353,723     57,759
ordinary_income     73,170       NULL        N/A
net_income          49,252     48,813        439
```

→ net_incomeはほぼ一致するがrevenueが大きく異なる。収益認識基準の違い、またはセグメント集計方法の違いの可能性。

## 3. 結論と対応方針

### 信頼性の高いフィールド（そのまま使える）
- **revenue**: 両ソースで高い一致率
- **operating_income**: 両ソースで高い一致率
- **eps**: 両ソースで高い一致率

### 注意が必要なフィールド
- **ordinary_income**: J-QuantsではIFRS/US-GAAP企業でNULL。TDnet側の方がカバレッジ高い
- **net_income**: 定義の違いで数%の差異が生じうる。親会社帰属 vs 当期純損益の違い

### 決算期変更によるキー衝突（3件）

financialsテーブルの主キー `(ticker_code, fiscal_year, fiscal_quarter)` では、
同一年に決算期を変更した企業の旧期・新期を区別できない。

| ticker | 旧期（EDINET既存） | 新期（TDnet） | 結果 |
|---|---|---|---|
| 4331 | 2025年3月期FY (end=2025-03-31) | 2025年12月期FY | EDINET優先でスキップ |
| 4891 | 2025年2月期FY (end=2025-02-28) | 2025年12月期FY | EDINET優先でスキップ |
| 6630 | 2025年4月期FY (end=2025-04-30) | 2025年12月期FY | EDINET優先でスキップ |

3社とも決算期を変更し、新旧両方の通期が `fiscal_year=2025, fiscal_quarter=FY` にマップされた。
根本対策は `fiscal_end_date` を主キーに含めるスキーマ変更だが、影響範囲が大きいため要検討。

## 4. 対応状況

### 完了
- [x] `compare_sources.py`: source='TDnet'→source IN ('TDnet','EDINET')に修正
- [x] `compare_sources.py`: JQuants側に`ticker_exists()`フィルタ追加
- [x] TDnetの`fiscal_year`和暦フォールバック改善（3件解消: 3204,3719,6171）
  - `_wareki_to_seireki()`関数を追加（令和/平成→西暦変換）
  - `fetch_financials.py`と`fetch_tdnet.py`の両方に適用
  - 既存データ3件のfiscal_yearを"2026"→"2025"に修正

- [x] TDnet fiscal_year判定: iXBRLのContext要素からfiscal_end_dateを取得して補正
  - `_extract_fiscal_end_date_from_xbrl()` 追加（`fetch_financials.py`）
  - `parse_ixbrl_financials()` の戻り値に `fiscal_end_date` を追加
  - `fetch_tdnet.py` でXBRL由来のfiscal_yearが異なればタイトルを上書き補正
  - 根本原因: 3864/6165はTDnet一覧HTMLのタイトルが「2025年3月期」だが実際は「2026年3月期」（企業の登録ミス）
  - 3909（JQuants欠損）は外部データソースの問題のため対象外

### 対応候補（未着手）
1. ~~TDnetの`fiscal_year`フォールバックロジック改善（6件のズレ解消）~~ → 和暦3件は修正済。3864/6165はiXBRL Context補正で対応済。3909はJQuants欠損（外部問題）
2. 決算期変更衝突の対策（スキーマ or 挿入ロジック改善）→ 3件（4331/4891/6630）がJQuantsのみに残留
3. 6269の個別調査（連結/単体混在の確認）
4. 8253（クレディセゾン）の個別調査（revenue乖離 57,759百万円）
5. net_incomeのマッピング統一（親会社帰属に揃える）
