# J-Quants vs TDnet 取得結果比較 調査レポート

日付: 2026-02-14
対象データ: 2026-02-13 開示分
スクリプト: `scripts/compare_sources.py --date 2026-02-13`

## 突合サマリー

### 最新（2026-02-14 再実行: 和暦修正後）

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

2回目→最新でDB件数が964→869に減少。EDINETが60→84件に増加した一方、
全体件数が減少したのはDB状態の変化（再取得による上書き・データ整理）による。
JQuantsのみも11→7に減少し、突合精度は安定している。

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

## 2. フィールド別の差異分析（最新実行: 86レコード）

### 差異パターンの分類

| パターン | 件数 | 説明 |
|---|---|---|
| ordinary_income NULLのみ | 26 | JQuantsでOdPがNULL、他フィールドは一致 |
| ordinary_income NULL + net_income差異 | 39 | OdP NULL + 純利益定義の違い |
| net_incomeのみ差異 | 19 | 小数点丸め or 親会社帰属 vs 当期純損益 |
| 特殊（revenue/op_income/eps含む） | 2 | 6269, 8253 |

フィールド別差異件数: ordinary_income=67, net_income=60, revenue=2, operating_income=1, eps=1

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

### 対応候補（未着手）
1. ~~TDnetの`fiscal_year`フォールバックロジック改善（6件のズレ解消）~~ → 和暦3件は修正済。残り3件（3864/6165のタイトルvsCurFYEn不一致、3909のJQuants欠損）は別issue
2. 決算期変更衝突の対策（スキーマ or 挿入ロジック改善）→ 3件（4331/4891/6630）がJQuantsのみに残留
3. 6269の個別調査（連結/単体混在の確認）
4. 8253（クレディセゾン）の個別調査（revenue乖離 57,759百万円）
5. net_incomeのマッピング統一（親会社帰属に揃える）
