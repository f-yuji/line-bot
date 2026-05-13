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

- シグナル発生時に `virtual_trades` へ仮想エントリーを記録
- 毎日 `check_virtual_trades.py` が出口条件をチェックして決済

### ケーステスト / バックテスト

- `trade_case_definitions` にルールセット（TP/SL・フィルタ・信用残条件など）を登録
- `trade_case_tester.py` が任意期間の候補銘柄に対してシミュレーション
- `backtest_case_mix.py` でケースを加重ミックスしたエクイティカーブを研究用に出力

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

`check_virtual_trades.py` が毎日実行し、以下の条件を優先順に確認する。

| 出口 | 条件 |
|---|---|
| Pullback利確 | エントリー後+2%以上上昇後、前日比-2%で決済 |
| RSIリバーサル | RSI75以上後にRSI低下で決済 |
| MA5割れ | 5日移動平均を割り込んで決済 |
| 損切 | エントリー価格から-4%で決済 |
| ボリュームフェード | 出来高比率が0.5未満で決済（プロキシ） |
| ATRトレーリング | ATR×乗数を下回ったら決済 |
| タイムアウト | 保有期限（デフォルト5日）で強制決済 |

保有期限は「高値更新なら延長」設定（デフォルト2日）で延長可能。

クリーンアップ決済（`cleanup_duplicate_open` / `cleanup_position_limit`）は別スクリプト（`cleanup_virtual_positions.py`）が担当し、パフォーマンス集計からは除外される。

---

## 信用残フィルタ

エントリー候補を `stock_weekly_margin_interest`（週次信用残）でフィルタする。

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
| **リバウンド判定** | 反発率、出来高倍率、RSI、MA5上抜け条件 |
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

## 旧機能メモ

- 「朝サマリー通知」「ポートフォリオ通知」は現在の運用では使わないためWeb UIで非表示。互換性のため設定カラムと `/web/portfolio` ルートは残存。
- LINE pushは原則停止し、確認はWeb UI中心。cronのリバウンド監視も `--no-notify` 推奨。
