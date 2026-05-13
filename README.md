# LINEニュースbot兼 急落リバウンドAI投資補助ツール

このリポジトリは、LINE Messaging API を入口にしたニュースbot、補助金検索bot、相場確認Web UI、東証プライム急落リバウンドAIを統合した個人向け運用ツールです。

自動売買はしません。銘柄候補の抽出、除外理由の提示、AI確率、期待値、仮想売買、検証用ラベル作成までを担当し、最終判断と発注は人間が行います。

## 主要機能

- LINEニュースbot
  - RSS/ニュース取得、要約、深掘り質問、直近ニュース文脈への回答。
  - 既存ニュースbot機能は投資AI側の「相場材料検知エンジン」としても使う方針。

- 補助金bot
  - 都道府県、業種別に補助金・助成金情報を返す。
  - LINE上でカテゴリ選択、続き表示、条件変更に対応。

- 急落検知
  - 日経225、東証プライム、Dow系の急落をスキャン。
  - 主対象は東証プライム。Dowは当面ログ保存のみで、日本株AIモデルには混ぜない。

- ルールベースのリバウンド監視
  - 急落銘柄を監視し、反発率、急落時からの戻り、出来高倍率、RSI回復、5日線上抜けを判定。
  - `early` / `confirmed` / `strong_confirmed` の `signal_stage` を保存。
  - 強悪材料は除外し、LINE通知しない。

- AIリバウンド予測
  - `stock_feature_snapshots` と `stock_rebound_labels` を使って LightGBM を学習。
  - 5営業日以内に+5%到達する確率を予測。
  - 期待値、相場モード、悪材料スコアを使ってシグナル段階を決める。
  - AIが使えない場合はルールベースへフォールバック。

- ニュース・相場モード
  - `market_news_signals` にニュース由来スコアを保存。
  - `market_regime` に `normal` / `shock` / `panic` / `recovery` を保存。
  - ニューススコア単独では `shock` にしない。市場実データの悪化条件を必須にする。

- 仮想売買
  - シグナル発生時に `virtual_trades` へ仮想エントリーを保存。
  - 利確+5%、損切-4%、最大5営業日を基本ルールに検証する。
  - `stock_drop_watchlist` は現在状態、`rebound_signal_history` は累積履歴として扱う。

- Web UI
  - LINE設定、ニュース、相場、ウォッチリスト、仮想売買、モデル情報などを確認する管理画面。

## 全体アーキテクチャ

```text
LINE Messaging API
  -> app.py
      -> ニュース応答 / 補助金応答 / 相場ボタン / Web UI

Supabase PostgreSQL
  -> users
  -> news_contexts / sent_articles / last_news_batch
  -> stock_drop_watchlist
  -> virtual_trades
  -> prime_stocks_cache
  -> stock_feature_snapshots
  -> stock_rebound_labels
  -> market_news_signals
  -> market_regime
  -> ml_models

Data Sources
  -> J-Quants Light: listed/info, daily_quotes, statements
  -> yfinance fallback
  -> RSS / Yahoo / Google News / Nikkei系RSS

ML Pipeline
  -> generate_feature_snapshots.py
  -> generate_rebound_labels.py
  -> train_rebound_model.py
  -> predict_rebound.py
```

## 急落リバウンドAIの流れ

```text
東証プライム銘柄一覧取得
  -> J-Quants優先、prime_stocks_cache保存

過去株価取得
  -> J-Quants daily_quotes優先、失敗時はyfinance fallback

特徴量生成
  -> stock_feature_snapshots

急落候補抽出
  -> is_drop_candidate = true

結果ラベル生成
  -> stock_rebound_labels
  -> t日終値から t+1〜t+5営業日を評価

LightGBM学習
  -> models/*.pkl
  -> ml_models

AI予測
  -> signal_probability
  -> expected_value
  -> signal_stage
  -> stock_drop_watchlist更新
  -> virtual_trades作成
  -> 必要時LINE通知
```

## シグナルライフサイクル

`signal_stage` はAI/ルール上の強さ、`status` はライフサイクル状態として分離します。

```text
watching
  -> 急落監視中

rebound_candidate + signal_stage=early
  -> 候補。監視強化段階。通常は virtual_trades を作成しない。

rebound_signal + signal_stage=confirmed
  -> 未エントリーの有効シグナル。

rebound_signal + signal_stage=strong_confirmed
  -> 強シグナル。entry_rank_limit / max_daily_entries / max_sector_positions は突破可能。

entered
  -> この watchlist から virtual_trade が作成済み。signal active から除外。

signal_skipped
  -> 上限、同一銘柄open、再エントリー冷却などでエントリー見送り。

expired / ai_dropped / closed / excluded
  -> 現在有効ではない終了状態。
```

Dashboard の `signal active` は以下のみを数えます。

```text
stock_drop_watchlist.status = 'rebound_signal'
signal_stage in ('confirmed', 'strong_confirmed')
is_excluded != true
virtual_trade_id is null
```

保有中は `virtual_trades.status='open' AND sell_date IS NULL` で判定します。

## 重要な判定ルール

### 急落候補

標準では以下を急落候補とします。

```text
day_change_pct <= -3.5
```

取引対象として最低限以下を満たす必要があります。

```text
close >= 100
turnover_value >= 100,000,000
```

### ラベル成功条件

対象日 `t` の終値を `entry_price` とします。

- 成功:
  - t+1〜t+5営業日の高値が `entry_price * 1.05` 以上に到達
  - その利確到達日より前に、終値ベースで `entry_price * 0.96` 以下へ到達していない

- 失敗:
  - 先に終値-4%へ到達
  - 5営業日以内に高値+5%へ到達しない
  - 同じ日に高値+5%と終値-4%が発生した場合も保守的に失敗

### AIシグナル

AI予測時の基本閾値です。

```text
none:
  probability < 0.55
  または expected_value <= 0
  または bad_news_score >= 80

early:
  probability >= 0.55
  expected_value > 0
  bad_news_score < 80

confirmed:
  probability >= 0.65
  expected_value > 0
  bad_news_score < 80

strong_confirmed:
  probability >= 0.72
  expected_value > 0
  bad_news_score < 60
  volume_ratio_20d >= 1.3
```

相場モードによって通知閾値は微調整します。

## ナフサ・エネルギー関連の扱い

ナフサ、原油高、樹脂、塗料、溶剤、防水材、建材などは `energy_naphtha_score` として保存します。

ただし、ナフサ関連だけを理由に銘柄を除外しません。将来AIが特徴量として判断できるように保存するだけです。

## J-Quants Light対応

J-Quantsを優先し、失敗時にyfinanceへフォールバックします。

利用する主なAPI:

- `/listed/info`
- `/prices/daily_quotes`
- `/fins/statements`

コードはJ-Quantsで5桁形式になる場合があります。

```text
72030 -> 7203
18010 -> 1801
```

普通株を優先し、ETF、REIT、インフラファンド、外国株などは可能な範囲で除外します。

## 主要スクリプト

### 銘柄一覧

```powershell
.\venv\Scripts\python.exe prime_stocks.py --refresh-jquants --dry-run
.\venv\Scripts\python.exe prime_stocks.py --refresh-jquants
```

### J-Quants接続確認

```powershell
.\venv\Scripts\python.exe scripts\test_jquants_light.py
```

### 特徴量生成

J-Quants優先で3年分を作ります。

```powershell
.\venv\Scripts\python.exe scripts\generate_feature_snapshots.py --years 3 --market prime --source jquants --limit 10 --dry-run
```

全件バックフィルは429対策のため分割・sleep付きで実行します。

```powershell
.\venv\Scripts\python.exe scripts\generate_feature_snapshots.py --years 3 --market prime --source jquants --skip-existing --sleep-seconds 3 --cooldown-on-429 600 *> backfill.log
```

途中再開:

```powershell
.\venv\Scripts\python.exe scripts\generate_feature_snapshots.py --years 3 --market prime --source jquants --start-after-code 4151 --skip-existing --sleep-seconds 5 --cooldown-on-429 900 *> backfill.log
```

### ラベル生成

長時間処理なのでログファイルへ出します。1000件ごとにDBへflushします。

```powershell
.\venv\Scripts\python.exe scripts\generate_rebound_labels.py --years 3 --limit 1000000 --progress-every 1000 --flush-every 1000 *> labels_flush.log
```

進捗確認:

```powershell
Select-String -Path labels_flush.log -Pattern "candidate load progress","candidates=","label progress","flush","upsert","summary","Traceback","ERROR" | Select-Object -Last 50
```

### ニューススコア

```powershell
.\venv\Scripts\python.exe scripts\generate_news_signals.py --today --dry-run
.\venv\Scripts\python.exe scripts\generate_news_signals.py --today --apply-to-features
```

### 相場モード更新

```powershell
.\venv\Scripts\python.exe scripts\update_market_regime.py --today --dry-run
.\venv\Scripts\python.exe scripts\update_market_regime.py --today --apply-to-features
```

### モデル学習

```powershell
.\venv\Scripts\python.exe scripts\train_rebound_model.py --years 3 --min-samples 300 --activate
```

### AI予測

```powershell
.\venv\Scripts\python.exe scripts\predict_rebound.py --latest --dry-run
.\venv\Scripts\python.exe scripts\predict_rebound.py --latest
.\venv\Scripts\python.exe scripts\predict_rebound.py --latest --notify
```

### 既存ルール監視

```powershell
.\venv\Scripts\python.exe scripts\monitor_rebound.py --dry-run
```

### 仮想売買チェック

```powershell
.\venv\Scripts\python.exe scripts\check_virtual_trades.py
```

## 主要DBテーブル

### stock_drop_watchlist

急落銘柄と現在の監視状態を保存します。

主なカラム:

- `code`
- `name`
- `status`
- `signal_stage`
- `signal_score`
- `signal_probability`
- `expected_value`
- `mode`
- `bad_news_score`
- `market_shock_score`
- `sector_risk_score`
- `fx_yen_score`
- `energy_naphtha_score`
- `interest_rate_score`
- `is_excluded`
- `exclude_reason`
- `last_signal_at`
- `signal_count`

### stock_feature_snapshots

1行が「銘柄コード × 日付」の特徴量です。LightGBMの入力になります。

主な特徴量:

- 株価、出来高、売買代金
- 前日比、急落率、5日/20日/52週高値からの下落率
- 移動平均乖離
- RSI
- 出来高倍率
- ATR、ボラティリティ
- 日経平均/TOPIX/VIXとの比較
- 財務指標
- ニューススコア
- `is_drop_candidate`
- `is_tradeable`

### stock_rebound_labels

急落候補の5営業日後までの成功/失敗ラベルです。

主なカラム:

- `feature_snapshot_id`
- `code`
- `trade_date`
- `entry_price`
- `future_high_1d` 〜 `future_high_5d`
- `future_low_1d` 〜 `future_low_5d`
- `future_close_1d` 〜 `future_close_5d`
- `max_return_5d_pct`
- `max_drawdown_5d_pct`
- `label_success`
- `label_reason`

### market_news_signals

ニュースをルールベースで分類し、市場・セクター・個別材料のスコアとして保存します。

主なスコア:

- `market_shock_score`
- `sector_risk_score`
- `bad_news_score`
- `fx_yen_score`
- `energy_naphtha_score`
- `interest_rate_score`
- `geopolitical_score`
- `supply_chain_score`

### market_regime

1日1行で相場モードを保存します。

モード:

- `normal`
- `shock`
- `panic`
- `recovery`

ニューススコアだけで `shock` にはせず、日経平均、TOPIX、値下がり比率、VIXなど市場実データの悪化を必須にします。

### ml_models

LightGBMモデルのバージョン、特徴量、評価指標、保存先を管理します。

### virtual_trades

AIまたはルールシグナルの仮想売買ログです。

既存DBカラム名に合わせ、エントリー日は `buy_date`、エントリー価格は `buy_price` を使います。

## 環境変数

`.env` に設定します。値はコードに直書きしません。

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

J-Quantsは原則 `JQUANTS_REFRESH_TOKEN` を使います。V2クライアント利用時はライブラリ側の認証方式に従います。

## 運用例

日次運用の目安です。最初はcronに入れず、手動確認を推奨します。

```text
08:30 ニューススコア生成
09:00 monitor-reboundで朝チェック
12:00 monitor-reboundで昼チェック
16:30 GitHub Actionsで決済チェック、stock_feature_snapshots生成、AI予測
週末 LightGBM再学習
```

## 開発・確認コマンド

構文チェック:

```powershell
.\venv\Scripts\python.exe -m py_compile app.py settings_loader.py scoring.py bad_news_filter.py scripts\monitor_rebound.py
.\venv\Scripts\python.exe -m py_compile scripts\generate_feature_snapshots.py scripts\generate_rebound_labels.py scripts\train_rebound_model.py scripts\predict_rebound.py
```

依存関係:

```powershell
.\venv\Scripts\pip.exe install -r requirements.txt
.\venv\Scripts\python.exe -c "import lightgbm, sklearn, joblib, numpy; print('ml imports ok')"
```

## デプロイ

Fly.ioを使います。

```powershell
fly deploy --remote-only
```

## 注意事項

- 自動売買は実装しない。
- Dowは日本株AIモデルに混ぜない。
- ナフサ関連だけで除外しない。
- 未来データリーク禁止。特徴量は対象日までの情報だけを使う。
- ラベル生成では未来データを使うが、目的変数としてのみ扱い、特徴量には混ぜない。
- 長時間処理はログファイルへ出力する。
- `--dry-run` ではDB保存しない。
- Supabaseの既存データは削除しない。
- J-Quants失敗時は可能な範囲でyfinance fallbackする。

## 旧機能メモ

- 「朝サマリー通知」と「ポートフォリオ通知」は、現在の急落リバウンドAI運用では使わないためWeb UIでは非表示にしています。
- 互換性のため、既存の設定カラムや `/web/portfolio` ルート自体は残しています。
- LINE pushは原則停止し、確認はWeb UI中心です。cronのリバウンド監視も `--no-notify` で実行します。
