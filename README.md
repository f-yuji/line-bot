# 急落リバウンドAI投資補助ツール（LINE bot統合）

東証プライム銘柄の急落リバウンドをAIで予測し、仮想売買で検証する個人向け運用ツール。  
自動売買はしない。候補抽出・AI確率・仮想売買・検証まで担当し、最終判断と発注は人間が行う。

LINE Messaging API を入口にしたニュースbot・補助金botも統合している。

---

## システム概要

```text
急落検知（scan_prime.py）
  → 特徴量生成（generate_feature_snapshots.py）
  → AI予測（predict_rebound.py）
      → シグナル判定（signal_stage）
      → 仮想売買エントリー（virtual_trades）
      → LINE通知（任意）

仮想売買チェック（check_virtual_trades.py）
  → 出口条件判定
  → 決済記録

バックテスト（backtest_case_mix.py）
  → ケース定義別シミュレーション
  → ミックス最適化検証
```

---

## 主要機能

### 急落検知・リバウンドAI

- 東証プライム全銘柄を対象に急落（day_change_pct ≤ -3.5%）を検知
- LightGBM で「5営業日以内に+5%到達する確率（signal_probability）」を予測
- 期待値・相場モード・悪材料スコアを組み合わせてシグナルステージを決定
- AIが使えない場合はルールベースへフォールバック

### 仮想売買

- AI本命以上が入口フィルタを通過すると、シグナル発生日の終値で `virtual_trades` へ仮想エントリーを記録
- 毎日 `check_virtual_trades.py` が出口条件をチェックして決済

### H5 Primary（現行の仮想売買Primary）

`h5_ai65_pb20_hd3_est12_cm_range330` が現行の仮想売買 Primary。2023〜2024 train / 2025〜 test で Forward-Test 実施済み。

| 条件 | 値 |
|---|---|
| AI確率 | ≥ 0.65 |
| signal_stage | confirmed / strong_confirmed |
| 20日高値からの下落 | ≤ -8% |
| 相場モード除外 | panic_selloff |
| 過熱スコア上限 | 1（cool/mild） |
| 緊急損切 | -12%（emergency stop） |
| 出口タイプ | peak_pullback_exit（-2% peak pullback） |
| 最大保有日数 | 3営業日 |
| 信用倍率 | 3〜30倍の範囲（range_3_30） |
| エントリー | シグナル当日の終値（同日 MOC 前提） |

エントリー注意: 翌日寄りは期待値が大幅低下（EV 47% 減）。**+2%超の GU は飛びつき警戒。**

### peak_pullback_exit の仕様

H5 Primary 専用の出口ロジック。既存の `pullback_exit`（当日終値前日比 -2%）とは別。

```text
1. SL チェック（優先）: 当日安値 <= entry × (1 + initial_sl_pct) なら SL 価格で決済
2. Peak 更新: 当日高値でピーク更新
3. Peak pullback: ピーク > entry × 1.005 かつ 当日終値 <= peak × 0.98 なら終値で決済
4. タイムアウト: max_holding_days（3日）経過で決済
```

### ケーステスト / バックテスト

- `trade_case_definitions` にルールセット（TP/SL・フィルタ・信用残条件など）を登録
- `trade_case_tester.py` が任意期間の候補銘柄に対してシミュレーション
- `backtest_case_mix.py` でケースを加重ミックスしたエクイティカーブを研究用に出力

### 重要: rebound_lab の戦略定義

`rebound_lab` は、名称から受ける印象とは異なり、**反発を確認してから買う戦略ではない**。

現在の入口は次のように動く。

```text
急落日の引け後に特徴量を生成
  → AIが「この終値から数営業日内に戻る可能性」を評価
  → confirmed / strong_confirmed が入口フィルタを通過
  → シグナル発生日の終値で仮想購入
```

つまり `rebound_signal` / `strong_confirmed` の意味は「反発確認済み」ではなく、**AIリバウンド予測候補**である。
大陰線で引けた銘柄、当日の終値が下落している銘柄、RSIが売られすぎでない銘柄でも、AI確率と入口フィルタを通れば仮想購入対象になり得る。

画面上の読み方:

| 表示 | 実際の意味 |
|---|---|
| 候補 (`early`) | AIの初動候補。通常は仮想購入しない |
| シグナル (`confirmed`) | AIリバウンド予測が本命水準。入口選抜対象 |
| 強本命 (`strong_confirmed`) | AIリバウンド予測が強水準。入口選抜で優先される |
| 仮想保有中 (`entered`) | シグナル日の終値で仮想購入済み |

`/web/settings` の「反発率」「RSI回復」「MA5上抜け」は `monitor_rebound.py` の観測判定用であり、日次の `predict_rebound.py` によるAI仮想購入の必須条件ではない。

この設計の長所は、カーリットのような反発前の底付近を拾える可能性があること。短所は、まだ下落途中の銘柄も拾うことである。実売買へ読み替える際は、「反発確認済み」ではなく「急落引けでのAI底拾い予測」として扱う。

### Web UI

| URL | 機能 |
|---|---|
| `/web/dashboard` | ウォッチリストサマリー、オープンポジション |
| `/web/watchlist` | 急落監視リスト（ステータスフィルタ付き） |
| `/web/signals` | 全シグナル表示 |
| `/web/virtual-trades` | 仮想売買ポジション一覧 |
| `/web/virtual-trades/performance` | 日次/週次/月次パフォーマンス集計 |
| `/web/virtual-trades/performance/detail` | 期間別決済明細 |
| `/web/case-tests` | ケース定義・テスト実行履歴 |
| `/web/case-tests/<run_id>/<case_id>` | ケーステスト詳細結果 |
| `/web/research-db` | リサーチDBスナップショット管理 |
| `/web/models` | MLモデル管理 |
| `/web/settings` | 戦略設定（後述） |

### LINE bot

- ニュースbot: RSS取得・要約・深掘り質問・文脈応答
- 補助金bot: 都道府県×業種別に補助金情報を返す

---

## AIシグナルの流れ

```text
LightGBM予測 → signal_probability（0〜1）
  ↓
_expected_value() = probability × TP% − (1−probability) × |SL%|
  ↓
signal_stage 判定（services/signal_stage.py）
```

### signal_stage 閾値（設定画面で変更可能）

| stage | 条件 | 意味 |
|---|---|---|
| `strong_confirmed` | probability ≥ 0.65 かつ rule_score ≥ 60 | 強本命 |
| `confirmed` | probability ≥ 0.50 | 本命 |
| `early` | probability ≥ 0.35 | 初動 |
| `none` | 上記未満 | シグナルなし |

- 相場モード（`ai_threshold_adjust`）で閾値を微調整
- `bad_news_score ≥ 80` はどのステージでも除外

### シグナルライフサイクル

```text
watching                      急落検知、監視開始
rebound_candidate (early)     候補。virtual_trade は原則作らない
rebound_signal (confirmed)    有効シグナル。エントリー対象
rebound_signal (strong)       強シグナル。上限チェックを一部突破可
entered                       virtual_trade 作成済み
signal_skipped                上限超過・冷却期間などでエントリー見送り
expired / closed / excluded   終了
```

---

## 仮想売買の出口ロジック

### H5 Primary（`is_primary_h5=true` のポジション）

`virtual_trade_exit.py` の H5 専用ロジックで処理。`peak_pullback_exit` を使う（上記参照）。

### 旧来の仮想売買（H5 以外）

`check_virtual_trades.py` が毎日実行し、以下の条件を優先順に確認する。

| 出口 | 条件 |
|---|---|
| GD損切 | 寄付きがエントリー価格から設定損切ライン以下なら、寄付き価格で決済 |
| 終値損切 | 終値がエントリー価格から設定損切率（デフォルト-4%）以下なら決済 |
| MA5回復後の再崩れ | 一度MA5以上へ回復した後、含み損かつMA5乖離が設定値以下なら決済 |
| RSI75後 pullback1 | RSIが設定過熱水準（デフォルト75）到達後、含み益中に前日比-1%以下なら決済 |
| pullback2 | RSI過熱到達前、含み益中に前日比-2%以下なら決済 |
| タイムアウト | 保有期限（デフォルト5営業日）で決済。ただし含み益中で直近高値更新なら延長可 |

保有期限は「高値更新なら延長」設定（デフォルト2日）で延長可能。

クリーンアップ決済（`cleanup_duplicate_open` / `cleanup_position_limit`）は別スクリプト（`cleanup_virtual_positions.py`）が担当し、パフォーマンス集計からは除外される。

---

## 信用残フィルタ

エントリー候補を `stock_weekly_margin_interest`（週次信用残）でフィルタする。

### H5 Primary の信用倍率設定

Forward-Test の結果、**range_3_30（3〜30倍）** が最良。根拠:

- 信用倍率が低すぎる（≤3倍）: 売り方がいないため、踏み上げのカタリストが働きにくい
- 信用倍率が高すぎる（>30倍）: 売り崩しリスクが高い
- le20（旧設定）は test 期間で no-filter より悪化（PF 3.956 vs 4.01）

### 旧来の仮想売買の信用倍率設定

| 設定項目 | デフォルト | 意味 |
|---|---|---|
| 信用倍率フィルタ 有効 | true | フィルタON/OFF |
| 信用倍率データ必須 | true | データなし銘柄を除外 |
| 信用倍率上限 | 5.0倍 | この倍率超の銘柄は除外 |

データは `import_jquants_margin.py` でJ-Quantsから取得。  
欠損チェックは `audit_missing_margin_for_case_mix.py` で実施可能。

---

## ケースミックスバックテスト

研究用。DBへの書き込みは行わない。

`trade_case_tester.py` と `backtest_case_mix.py` の rebound ケースは、現行の日次AI入口と同じく**シグナル発生日の終値を入口価格**として評価する。翌営業日の寄付き購入や、反発確認後の購入は検証していない。

主に比較したもの:

| 比較軸 | 内容 |
|---|---|
| 入口選抜 | 現行入口、AI上位、期待値上位、保有数制限、セクター制限、地合い厳格化 |
| 信用倍率 | 5倍以下、10倍以下、20倍以下など |
| 出口 | 固定利確、pullback2、MA割れ、RSI反落、トレーリングなど |

比較テストで強かった `pullback2` は、**含み益中に当日終値が前日終値比-2%以下になったら終値で決済**するルールであり、最高値から-2%のトレーリングではない。

注意: 比較テスト当時の `pullback2` の初期損切は日中安値での固定損切を使っており、現行仮想売買の終値損切・GD区分・RSI75後pullback1・MA5再崩れ追加後の出口とは完全同一ではない。入口の前提は近いが、出口込みの運用成績は別途確認する必要がある。

```powershell
# 全シナリオ・全ミックスを実行
.\venv\Scripts\python.exe scripts\backtest_case_mix.py --scenario all --mix all

# レポート生成（PNG + CSV + Markdown）
.\venv\Scripts\python.exe scripts\render_case_mix_report.py
```

出力先: `outputs/case_mix/`

定義済みシナリオ: `2020_covid_crash` / `2022_rate_hike_bear` / `2023_rebound` / `2024_ai_bubble` / `2025_ai_bubble` / `custom_recent`

定義済みミックス: `core_mix` / `pullback2_only` / `defensive_mix` / `bull_mix`

---

## MLパイプライン

```text
generate_feature_snapshots.py   特徴量生成（stock_feature_snapshots）
generate_rebound_labels.py      結果ラベル生成（stock_rebound_labels）
train_rebound_model.py          LightGBM学習・保存（ml_models）
predict_rebound.py              AI予測・シグナル更新・仮想売買作成
```

### 急落候補の条件

```text
day_change_pct <= -3.5%
close >= 100円
turnover_value >= 1億円
```

### ラベル成功条件

`t` 日終値（entry_price）を起点に：

- **成功**: t+1〜t+5営業日の高値が entry_price × 1.05 以上に到達、かつそれより前に終値-4%未到達
- **失敗**: 先に終値-4%到達 / 5日以内に高値+5%未到達 / 同日に両方発生（保守的に失敗）

---

## 戦略設定（Web UI）

`/web/settings` から変更可能。DBの `strategy_settings` テーブルに保存。

| セクション | 主な設定項目 |
|---|---|
| **急落検知** | watchlist登録閾値(-3.5%)、指数乖離閾値(-2.0%pt) |
| **リバウンド判定** | 反発率、出来高倍率、RSI、MA5上抜け条件（`monitor_rebound.py` の観測判定用。日次AI入口の必須条件ではない） |
| **エントリー選抜** | 最大保有数(20)、1日上限(5)、上位ランク数(10)、同セクター上限(2) |
| **仮想売買の出口** | 反落利確%、RSIライン、損切%、保有日数 |
| **エントリーフィルタ** | 信用倍率フィルタ・上限(5.0倍) |
| **ルールスコア配点** | テクニカル50/ファンダ30/地合い20、監視閾値 |
| **AI予測** | AI予測ON/OFF、early/confirmed/strong確率閾値 |
| **通知設定** | 急落通知、リバ通知、AI通知、早期通知、push通知閾値 |

---

## 主要DBテーブル

| テーブル | 用途 |
|---|---|
| `stock_drop_watchlist` | 急落銘柄と現在の監視状態 |
| `rebound_signal_history` | シグナル発生履歴（累積ログ） |
| `virtual_trades` | 仮想売買ログ（buy_date / sell_date / profit_loss） |
| `stock_feature_snapshots` | LightGBM入力の特徴量（銘柄×日付） |
| `stock_rebound_labels` | 5日後までの高値・安値・終値ラベル |
| `stock_weekly_margin_interest` | 週次信用残（code / date / margin_ratio） |
| `ml_models` | 学習済みモデルのメタ情報・パス・is_active |
| `market_regime` | 1日1行の相場モード（normal/shock/panic/recovery） |
| `market_news_signals` | ニュース由来の各種スコア |
| `strategy_settings` | 戦略設定（user_id='global'の1行） |
| `trade_case_definitions` | ケーステストのルール定義 |
| `trade_case_runs` | ケーステスト実行ログ |
| `trade_case_results` | ケーステスト集計結果 |
| `trade_case_simulations` | ケーステスト個別トレード記録 |
| `prime_stocks_cache` | 東証プライム銘柄キャッシュ |
| `research_datasets` / `research_case_snapshots` | リサーチDB |
| `sent_articles` | ニュースbot 送信済み記事の重複チェック用 |
| `news_contexts` | ニュースbot 会話コンテキスト |
| `article_summaries` | ニュースbot 記事要約キャッシュ |
| `last_news_batch` | ニュースbot 最終バッチ記録 |

---

## 主要スクリプト

### 日次運用

```powershell
# 朝: ニューススコア生成
.\venv\Scripts\python.exe scripts\generate_news_signals.py --today

# 朝/昼: リバウンド監視
.\venv\Scripts\python.exe scripts\monitor_rebound.py --dry-run
.\venv\Scripts\python.exe scripts\monitor_rebound.py

# 夕方: 特徴量生成 → AI予測 → 決済チェック
.\venv\Scripts\python.exe scripts\generate_feature_snapshots.py --today --source jquants
.\venv\Scripts\python.exe scripts\predict_rebound.py --latest
.\venv\Scripts\python.exe scripts\check_virtual_trades.py
```

### 週次・不定期

```powershell
# モデル再学習
.\venv\Scripts\python.exe scripts\train_rebound_model.py --years 3 --min-samples 300 --activate

# 信用残インポート
.\venv\Scripts\python.exe scripts\import_jquants_margin.py

# ケースミックスバックテスト
.\venv\Scripts\python.exe scripts\backtest_case_mix.py --scenario all --mix all
```

### H5 Forward-Test / 分析

```powershell
# H5 Primary + 比較4ケースの train/test Forward-Test
.\venv\Scripts\python.exe scripts\run_h5_forward_test.py               # train + test 両方
.\venv\Scripts\python.exe scripts\run_h5_forward_test.py --period test  # 2025-01-01〜のみ
.\venv\Scripts\python.exe scripts\run_h5_forward_test.py --period train # 〜2024-12-31のみ

# H5 詳細分析（SL深度・信用残・ポジション制約・entry lag・最終候補比較）
.\venv\Scripts\python.exe scripts\analyze_h5_forward_next_steps.py
# 出力: outputs\rebound_next_analysis\h5_forward_next\

# H5 ケース定義を Supabase に登録（初回・定義変更時）
.\venv\Scripts\python.exe scripts\register_h5_cases.py
```

### バックフィル（初回・大量処理）

```powershell
# 特徴量バックフィル（3年分・429対策つき）
.\venv\Scripts\python.exe scripts\generate_feature_snapshots.py --years 3 --market prime --source jquants --skip-existing --sleep-seconds 3 --cooldown-on-429 600 *> backfill.log

# ラベル生成
.\venv\Scripts\python.exe scripts\generate_rebound_labels.py --years 3 --limit 1000000 --progress-every 1000 --flush-every 1000 *> labels.log
```

---

## データソース

- **J-Quants Light（優先）**: `/listed/info`, `/prices/daily_quotes`, `/fins/statements`, 週次信用残
- **yfinance（フォールバック）**: J-Quants失敗時
- **RSS/Yahoo/Google News/Nikkei系RSS**: ニュースbot用

J-Quantsのコードは5桁の場合がある（`72030` → `7203`）。普通株を優先し、ETF/REIT/外国株は除外。

---

## 環境変数（.env）

```text
SUPABASE_URL=
SUPABASE_KEY=

LINE_CHANNEL_ACCESS_TOKEN=
LINE_CHANNEL_SECRET=

OPENAI_API_KEY=

JQUANTS_REFRESH_TOKEN=
JQUANTS_EMAIL=
JQUANTS_PASSWORD=

WEB_URL=
```

---

## デプロイ

Fly.io を使用。

```powershell
fly deploy --remote-only
```

---

## 注意事項

- **自動売買は実装しない。** 最終判断と発注は人間が行う。
- **未来データリーク禁止。** 特徴量は対象日までの情報だけを使う。ラベル生成では未来データを目的変数としてのみ使用。
- **Dowは日本株AIモデルに混ぜない。** ログ保存のみ。
- **ナフサ関連だけで銘柄除外しない。** 特徴量として保存するだけ。
- `--dry-run` ではDB保存しない。
- Supabaseの既存データは削除しない。
- 長時間処理はログファイルへ出力する。

### 期待値（expected_value）について

現在の `expected_value` は固定TP/SL前提の簡易計算値。UI表示・ランキング補助・ログ用途のみで使用し、エントリーフィルタとしては使っていない。実運用出口（pullback / RSI / MA5 など）とは前提が異なるため、実績データが蓄積されてから再検討する。

---

## 地合い判定と入口モードの整理

ダッシュボードでは、地合いを「短期」と「長期」に分けて見る。

### 短期地合い

短期地合いは、数日から1週間程度の需給を見る。

用途は、入口モードを毎日細かく切り替えることではない。当日の危険度チェックとして使う。

- `normal`: 通常
- `risk_off`: 短期的にやや警戒
- `panic_selloff`: 全面安に近い。新規停止ガード候補
- `panic_rebound`: パニック後の反発局面。深リバ優先候補

短期スコアは推奨入口と直接対応しないため、UIでは細かいスコア表示を省略する。代わりに「短期ガード」が発動しているかだけを見る。

### 長期地合い

長期地合いは、半年から1年目線の市場構造を見る。入口モードの基本方針を決める材料。

主な材料:

- 日経平均 / TOPIX の200日移動平均位置
- 52週高値位置
- 最新日の全銘柄のMA25上銘柄率
- 最新日の全銘柄のMA75上銘柄率
- VIX

`MA25上 47%` は、最新スナップショット内の銘柄のうち、終値が25日移動平均より上にある銘柄が47%という意味。

`日経200MA 上(21.5%)` は、日経平均が200日移動平均より21.5%上にあるという意味。

長期地合いの主な表示:

| regime | UI表示 | 読み方 |
|---|---|---|
| `secular_risk_on` | 長期上昇基調 | 市場全体に買いが広がりやすく、押し目型が機能しやすい |
| `late_bull` | 上昇継続・やや過熱 | 指数は強いが過熱感あり。押し目は有効でも飛び乗り注意 |
| `distribution` | 指数は耐えるが個別は弱い | 指数は強い銘柄に支えられるが、個別全体には買いが広がっていない |
| `secular_bear` | 長期下落基調 | 浅い押し目より、深く売られた後のリバウンドを慎重に見る |
| `panic_crisis` | 危機的な全面安 | 新規停止を優先 |
| `neutral` | 中立 | normal標準型を基本に確認 |

### 入口モード

入口モードは、実際に仮想売買エントリー条件へ反映される入口フィルタ。

| mode | 意味 |
|---|---|
| `normal` | 現行標準ロジック |
| `risk_on_pullback` | MA5上側の浅い押し目を重視 |
| `panic_deep_rebound` | MA5下側の深い急落リバを重視 |
| `paused` | 新規エントリー停止 |
| `auto` | 推奨入口を実効入口として使う実験モード |

推奨入口は、長期地合いを基本に決める。

```text
secular_risk_on / late_bull / distribution
→ risk_on押し目型

secular_bear
→ panic深リバ型

panic_crisis
→ 新規停止

neutral
→ normal標準型
```

短期地合いは、通常時に入口をコロコロ変えるためには使わない。危険時のガードとしてだけ使う。

```text
短期 panic_selloff
→ 新規停止推奨

短期 panic_rebound
→ panic深リバ型推奨

短期 risk_off かつ 長期がrisk_on押し目型
→ normal標準型へ一段慎重化
```

UI上の意味:

- `推奨入口`: 長期地合い + 短期ガードから出したAI側のおすすめ
- `推奨基準`: 推奨入口の主な根拠。基本は長期地合い
- `短期ガード`: 短期地合いによるブレーキが発動しているか
- `現在設定中の入口モード`: 設定画面で人間が選んでいる値
- `実効入口モード`: 実際に仮想売買で使われる入口モード

重要:

`normal` を手動設定している場合、実効入口は `normal` のまま。  
`auto` を選んだ場合だけ、推奨入口が実効入口になる。

---

## 旧機能メモ

- 「朝サマリー通知」「ポートフォリオ通知」は現在の運用では使わないためWeb UIで非表示。互換性のため設定カラムと `/web/portfolio` ルートは残存。
- LINE pushは原則停止し、確認はWeb UI中心。cronのリバウンド監視も `--no-notify` 推奨。
