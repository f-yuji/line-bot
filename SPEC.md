# 投資補助ツール 仕様書

## 目次

1. [システム概要](#1-システム概要)
2. [全体アーキテクチャ](#2-全体アーキテクチャ)
3. [データフロー](#3-データフロー)
4. [急落検知](#4-急落検知)
5. [リバウンド監視](#5-リバウンド監視)
6. [スコアリングロジック](#6-スコアリングロジック)
7. [悪材料フィルター](#7-悪材料フィルター)
8. [仮想売買](#8-仮想売買)
9. [東証プライム全銘柄スキャン](#9-東証プライム全銘柄スキャン)
10. [LINE通知](#10-line通知)
11. [Web UI](#11-web-ui)
12. [設定パラメータ](#12-設定パラメータ)
13. [データベース設計](#13-データベース設計)
14. [cronスケジュール](#14-cronスケジュール)
15. [デプロイ構成](#15-デプロイ構成)

---

## 1. システム概要

日経225および東証プライム銘柄の急落を検知し、リバウンド候補をスコアリングしてLINEで通知する個人向け投資補助ツール。

**主な機能:**
- 平日の市場時間中に急落銘柄を自動検知
- 急落後の反発シグナルをテクニカル・ファンダメンタル・地合いで総合スコアリング
- 悪材料（不祥事・下方修正等）が原因の急落はフィルタリング
- シグナル発生時にLINEへ1通まとめ通知
- Webダッシュボードで詳細分析（スコア内訳・チャート・財務指標）
- 仮想売買で戦略のバックトラッキング

---

## 2. 全体アーキテクチャ

```
GitHub Actions (cron)
    │
    ├── 08:00 JST  nikkei_alert.py --fetch-financials  → 財務データ取得
    ├── 14:00 JST  nikkei_alert.py                     → 日経225急落検知
    ├── 15:30 JST  scripts/scan_prime.py               → 東証プライム急落スキャン
    ├── 15:40 JST  scripts/scan_prime.py               → 同上
    ├── 09:00/12:00/15:30/18:00 JST
    │              scripts/monitor_rebound.py           → リバウンド監視
    │
    ▼
Supabase (PostgreSQL)
    │
    ├── stock_drop_watchlist   急落銘柄・監視状態
    ├── strategy_settings      戦略パラメータ
    ├── virtual_trades         仮想売買記録
    ├── prime_stocks_cache     プライム銘柄キャッシュ
    ├── nikkei_financials      財務データキャッシュ
    └── ...（その他）
    │
    ▼
Fly.io (Flask app)
    ├── /callback              LINE Webhook
    └── /web/*                 Web管理UI（認証あり）
```

---

## 3. データフロー

```
急落検知（nikkei_alert / scan_prime）
    │
    └── stock_drop_watchlist に INSERT（status: watching）
            │
            ▼
    リバウンド監視（monitor_rebound）が平日4回チェック
            │
            ├── シグナルなし → last_checked_at 更新のみ
            │
            └── シグナルあり
                    │
                    ├── スコア計算 → DB更新
                    ├── 悪材料チェック（条件付き）
                    │       └── 悪材料あり → スキップ（has_bad_news=True）
                    │
                    ├── LINE通知（1通にまとめ）
                    ├── status: notified に更新
                    └── スコア≥80 → 仮想買いポジション作成
```

---

## 4. 急落検知

### 4-1. 日経225（nikkei_alert.py）

**実行タイミング:** 毎平日 14:00 JST

**処理:**
1. yfinance で日経225構成225銘柄の当日株価を取得
2. 前日終値比で変化率（`change_pct`）を計算
3. 以下の条件で急落・通知判定:

| 判定 | 条件 |
|------|------|
| ウォッチリスト登録 | `change_pct ≤ drop_list_threshold`（デフォルト -2.0%） |
| LINE急落速報 | `change_pct ≤ alert_threshold`（デフォルト -9.0%）かつ 日経比で `index_gap_threshold`（-1.5%）以上乖離 |

4. ウォッチリスト登録時、同一銘柄が `watching` / `rebound_signal` / `notified` で存在する場合はスキップ（重複防止）

### 4-2. 財務データ取得（--fetch-financials）

**実行タイミング:** 毎平日 08:00 JST

J-Quants APIから225銘柄の財務データ（PER・PBR・配当利回り・赤字フラグ等）を取得し `nikkei_financials` テーブルにキャッシュ。

---

## 5. リバウンド監視

**ファイル:** `scripts/monitor_rebound.py`

**実行タイミング:** 毎平日 09:00 / 12:00 / 15:30 / 18:00 JST

### 5-1. 処理フロー

```
stock_drop_watchlist から status IN (watching, rebound_signal) を取得
    │
    ├── 監視期限チェック（watch_days_limit 営業日を超えたら closed）
    │
    ├── yfinance で直近3ヶ月の日足を取得
    │
    ├── リバウンドシグナルチェック（check_rebound）
    │       ├── ① 前日比が daily_rebound_threshold（3.0%）以上
    │       ├── ② MA5 上抜け（ma5_cross_enabled=True の場合）
    │       └── ③ 急落時株価から drop_rebound_threshold（5.0%）以上回復
    │
    ├── スコア計算（6章参照）
    │
    ├── 悪材料チェック（スコア≥watch_score かつシグナルあり の場合のみ）
    │
    └── シグナルあり & 悪材料なし → LINE通知 + status: notified
```

### 5-2. リバウンドシグナル条件

シグナルは以下の3条件のいずれかを満たすと発生。2条件以上で「強シグナル★★」判定。

| # | 条件 | パラメータ |
|---|------|-----------|
| ① | 前日比リバウンド率 ≥ 閾値 | `daily_rebound_threshold`（デフォルト 3.0%）|
| ② | 5日移動平均線を下から上抜け | `ma5_cross_enabled`（デフォルト true）|
| ③ | 急落時株価からの回復率 ≥ 閾値 | `drop_rebound_threshold`（デフォルト 5.0%）|

---

## 6. スコアリングロジック

**ファイル:** `scoring.py`

**満点:** 100点 = テクニカル(50) + ファンダメンタル(30) + 地合い(20)

### 6-1. テクニカルスコア（0〜50点）

| 項目 | 最大 | 計算方法 |
|------|------|---------|
| MA5上抜け | +10点 | 前日終値 ≤ MA5 かつ 当日終値 > MA5 で加点（ma5_cross_enabled=true 時のみ）|
| 前日比リバウンド | 0〜10点 | `day_pct / daily_rebound_threshold × 10`（上限10点）|
| 急落時比回復 | 0〜15点 | `from_drop / drop_rebound_threshold × 15`（上限15点）|
| 出来高急増 | 0〜10点 | `(vol_ratio - 1) / (volume_ratio_threshold - 1) × 10`（上限10点）|
| RSI回復 | +5点 | RSI(14)が`rsi_low_threshold`(30)以下から`rsi_recover_threshold`(35)以上に回復で加点 |

**出来高計算:** 直近20日平均との比率。`volume_ratio_threshold`（デフォルト1.5倍）以上で満点方向。

### 6-2. ファンダメンタルスコア（0〜30点）

**赤字企業は0点固定。**

| 指標 | 最大 | 基準 |
|------|------|------|
| PER | 10点 | <10倍:10点 / <15倍:8点 / <20倍:5点 / 以上:2点 / 情報なし:5点 |
| PBR | 10点 | <1.0倍:10点 / <1.5倍:8点 / <2.0倍:5点 / 以上:2点 / 情報なし:5点 |
| 配当利回り | 10点 | ≥3%:10点 / ≥2%:7点 / ≥1%:4点 / <1%:1点 / 情報なし:3点 |

### 6-3. 地合いスコア（0〜20点）

急落日の日経平均変動率（`nikkei_pct`）で判定。市場全体が下げているほど「パニック売り→反発期待」として高得点。

| 日経変動率 | 得点 |
|-----------|------|
| -2.0% 以下 | 20点 |
| -1.0% 以下 | 14点 |
| 0% 以下 | 8点 |
| 0% 超（上昇日） | 4点 |
| 情報なし | 10点（中立） |

### 6-4. スコアラベル

| スコア | ラベル |
|--------|--------|
| ≥ `strong_watch_score`（デフォルト80点） | 強監視★★ |
| ≥ `watch_score`（デフォルト70点） | 監視 |
| ≥ `ignore_score`（デフォルト60点） | 観察 |
| それ以下 | スルー |

---

## 7. 悪材料フィルター

**ファイル:** `bad_news_filter.py`

### 対象条件（以下を両方満たす場合のみAPIを叩く）

- 急落率 ≤ -15%（個別要因の大幅下落）
- 急落日の日経変動率 > -3%（市場全体は安定）

→ この条件を満たさない場合（市場全体の下げで急落した場合等）はAPIを叩かず `has_bad_news=False`

### 判定方法

yfinance のニュースAPIで直近5件のタイトルを取得し、以下キーワードを検索:

```
不祥事 / 不正 / 粉飾 / 下方修正 / 業績悪化 / 赤字転落 / 倒産 / 民事再生 /
上場廃止 / 行政処分 / 課徴金 / 逮捕 / 虚偽記載 / リコール / 自主回収 /
大幅減益 / 経営危機 / 債務超過 / 特別損失 / 不正会計 / 損失計上
```

キーワードが1つでもヒット → `has_bad_news=True` → LINE通知スキップ（WebUIに⚠表示）

---

## 8. 仮想売買

**テーブル:** `virtual_trades`

スコアが `strong_watch_score`（デフォルト80点）以上のシグナル銘柄を自動で仮想買い。実際の発注はしない。

### 売買ルール

| 条件 | アクション |
|------|----------|
| スコア ≥ 80点 & シグナル発生 | 仮想買い（同一銘柄の open ポジションが既にあればスキップ）|
| 現在価格が買値 +10% | 利確（take_profit）|
| 現在価格が買値 -7% | 損切（stop_loss）|
| `watch_days_limit` 営業日経過 | 期限切れ（expired）|

### 決済タイミング

`monitor_rebound.py` の各実行時（1日4回）に保有ポジションをチェック。

### WebUI表示

`/web/virtual-trades` で確認可能:
- 保有中ポジション一覧
- 決済済みトレード履歴（買値・売値・損益・損益率・決済理由）
- 累計損益・勝敗カウント

---

## 9. 東証プライム全銘柄スキャン

**ファイル:** `scripts/scan_prime.py` / `prime_stocks.py`

**実行タイミング:** 毎平日 15:30 / 15:40 JST

### 処理フロー

1. `prime_stocks_cache` テーブルから銘柄リストを取得（最終更新から7日以内）
2. キャッシュ切れまたは月曜日 → J-Quants API `/v2/listed/info` から再取得・保存
3. J-Quants未設定時はNIKKEI225フォールバック
4. 日経225銘柄はスキップ（`nikkei_alert.py` が担当）
5. 200銘柄ずつ yfinance バッチ取得
6. `drop_list_threshold` 以下の銘柄を watchlist に保存
7. `alert_threshold` 以下の銘柄（最大10件）をLINE速報送信

### J-Quants API 認証

```
JQUANTS_API_KEY（リフレッシュトークン）
    → POST /v2/token/auth_refresh
    → idToken 取得
    → GET /v2/listed/info（Bearer認証）
```

---

## 10. LINE通知

### リバウンド候補通知（1通）

複数銘柄のシグナルが発生しても**1通にまとめて送信**。スコア降順でソートし、最高スコア銘柄を詳細表示。

```
⚡ リバウンド候補
7203 トヨタ自動車
急落 -10.0%　スコア 85点（強監視★★）
判定: 強シグナル★★
他 3銘柄: 5214 / 8802 / 9503
詳細 → https://line-bot-ukz5kw.fly.dev/web/dashboard
```

### 急落速報

`alert_threshold`（-9.0%）以下の急落検知時に即送信（nikkei_alert / scan_prime）。

### 通知制御設定

| 設定 | デフォルト | 説明 |
|------|----------|------|
| `rebound_notify_enabled` | true | リバウンド通知のON/OFF |
| `drop_notify_enabled` | true | 急落速報のON/OFF |

---

## 11. Web UI

**認証:** `WEB_ADMIN_TOKEN`（環境変数）をトークン入力してログイン

| ページ | URL | 説明 |
|--------|-----|------|
| ダッシュボード | `/web/dashboard` | シグナル発生と監視中を分けて表示。行クリックで詳細モーダル |
| ウォッチリスト | `/web/watchlist` | 全ステータス一覧・フィルタ・クローズ操作 |
| シグナル | `/web/signals` | 通知済み銘柄の履歴 |
| 仮想売買 | `/web/virtual-trades` | 仮想トレード実績 |
| 設定 | `/web/settings` | strategy_settings の編集 |

### 銘柄詳細モーダル

ダッシュボード・ウォッチリストで行をクリックすると表示:
- スコア合計 + ラベル + テクニカル/ファンダ/地合い内訳
- 悪材料警告（has_bad_news=true の場合）
- セクター・急落日・急落率・急落時株価・日経乖離
- PER・PBR・配当利回り
- 直近10日の実株価推移チャート

### スマートフォン対応

幅768px以下でモバイルレイアウト（ハンバーガーメニュー → スライドサイドバー）。ホーム画面への追加でアプリとして動作（Apple Touch Icon設定済み）。

---

## 12. 設定パラメータ

`/web/settings` または Supabase の `strategy_settings` テーブル（user_id="global"）で管理。

### 急落検知

| パラメータ | デフォルト | 説明 |
|-----------|----------|------|
| `drop_list_threshold` | -2.0% | ウォッチリスト登録の閾値 |
| `alert_threshold` | -9.0% | LINE急落速報の閾値 |
| `index_gap_threshold` | -1.5% | 日経との乖離でLINE速報する閾値 |

### リバウンドシグナル

| パラメータ | デフォルト | 説明 |
|-----------|----------|------|
| `daily_rebound_threshold` | 3.0% | シグナル①：前日比リバウンド率 |
| `drop_rebound_threshold` | 5.0% | シグナル③：急落時株価からの回復率 |
| `ma5_cross_enabled` | true | シグナル②：MA5上抜け判定の使用 |
| `volume_ratio_threshold` | 1.5倍 | 出来高急増の判定倍率 |
| `rsi_low_threshold` | 30.0 | RSI回復判定：過去の最低ライン |
| `rsi_recover_threshold` | 35.0 | RSI回復判定：回復ライン |

### スコアリング閾値

| パラメータ | デフォルト | 説明 |
|-----------|----------|------|
| `strong_watch_score` | 80点 | 強監視★★ / 仮想買い実行の閾値 |
| `watch_score` | 70点 | 監視ラベル / 悪材料チェック実行の閾値 |
| `ignore_score` | 60点 | 観察ラベルの閾値 |

### 監視管理

| パラメータ | デフォルト | 説明 |
|-----------|----------|------|
| `watch_days_limit` | 10営業日 | この日数を超えたら自動クローズ |

---

## 13. データベース設計

### stock_drop_watchlist（主テーブル）

| カラム | 型 | 説明 |
|--------|-----|------|
| id | uuid | PK |
| code | text | 銘柄コード（例: 7203）|
| name | text | 銘柄名 |
| market | text | nikkei225 / prime |
| drop_detected_at | timestamptz | 急落検知日時 |
| drop_pct | float | 急落率（例: -10.5）|
| price_at_drop | float | 急落時株価 |
| nikkei_pct | float | 急落日の日経変動率 |
| sector | text | セクター |
| status | text | watching / rebound_signal / notified / closed |
| score | float | 総合スコア（0〜100）|
| score_technical | float | テクニカルスコア（0〜50）|
| score_fundamental | float | ファンダスコア（0〜30）|
| score_market | float | 地合いスコア（0〜20）|
| score_label | text | 強監視★★ / 監視 / 観察 / スルー |
| has_bad_news | bool | 悪材料フラグ |
| per | float | PER |
| pbr | float | PBR |
| div_yield_pct | float | 配当利回り（%）|
| price_history | jsonb | 直近10日終値リスト |
| rebound_notified_at | timestamptz | LINE通知日時 |
| last_checked_at | timestamptz | 最終監視日時 |

### virtual_trades

| カラム | 型 | 説明 |
|--------|-----|------|
| id | uuid | PK |
| watchlist_id | uuid | stock_drop_watchlist FK |
| code | text | 銘柄コード |
| buy_price | float | 仮想買値 |
| buy_date | timestamptz | 仮想買い日時 |
| sell_price | float | 仮想売値 |
| sell_date | timestamptz | 仮想決済日時 |
| sell_reason | text | take_profit / stop_loss / expired / manual |
| profit_loss | float | 損益（円）|
| profit_loss_pct | float | 損益率（%）|
| status | text | open / closed |

---

## 14. cronスケジュール

`.github/workflows/cron.yml` で定義（GitHub Actions）。時刻はUTC表記、JST=UTC+9。

| UTC | JST | ジョブ | 処理 |
|-----|-----|--------|------|
| `0 23 * * 0-4` | 08:00 (月〜金) | run-financials | 財務データ取得（J-Quants）|
| `0 0 * * 1-5` | 09:00 (月〜金) | monitor-rebound | リバウンド監視 |
| `0 3 * * 1-5` | 12:00 (月〜金) | monitor-rebound | リバウンド監視 |
| `30 7 * * 1-5` | 16:30 (月〜金) | rebound-ai-daily | 仮想売買決済チェック、特徴量生成、AI予測 |
| `0 12 * * 6` | 21:00 (土) | train-rebound-models | 5d/10dモデル再学習 |

**テスト実行:** `.env` に `ENV=test` を追加すると土日の平日チェックをバイパス。

---

## 15. デプロイ構成

### Fly.io（Webサーバー）

```toml
# fly.toml
app = "line-bot-ukz5kw"
primary_region = "nrt"  # 東京リージョン
```

**必要なシークレット:**

```bash
fly secrets set SUPABASE_URL=...
fly secrets set SUPABASE_KEY=...
fly secrets set LINE_CHANNEL_ACCESS_TOKEN=...
fly secrets set LINE_CHANNEL_SECRET=...
fly secrets set OPENAI_API_KEY=...
fly secrets set WEB_ADMIN_TOKEN=...
fly secrets set JQUANTS_API_KEY=...   # 任意（未設定時はNIKKEI225フォールバック）
```

**デプロイ:**

```bash
fly deploy --remote-only
```

### GitHub Actions（cron実行）

**必要なシークレット（GitHub repository secrets）:**

- `SUPABASE_URL`
- `SUPABASE_KEY`
- `LINE_CHANNEL_ACCESS_TOKEN`
- `JQUANTS_API_KEY`（任意）
- `OPENAI_API_KEY`（monitor_rebound で使用）

### 環境変数

| 変数 | 説明 |
|------|------|
| `SUPABASE_URL` | Supabase プロジェクトURL |
| `SUPABASE_KEY` | Supabase anon key |
| `LINE_CHANNEL_ACCESS_TOKEN` | LINE Messaging API |
| `LINE_CHANNEL_SECRET` | LINE Webhook署名検証 |
| `OPENAI_API_KEY` | AIコメント生成（任意）|
| `WEB_ADMIN_TOKEN` | Web UI ログイントークン |
| `JQUANTS_API_KEY` | J-Quants APIリフレッシュトークン |
| `WEB_URL` | LINE通知に貼るURL（デフォルト: Fly.io URL）|
| `ENV` | `test` 設定で土日スキップを無効化 |
| `SUPABASE_MODE` | `prod` / `test` でDB切り替え |
