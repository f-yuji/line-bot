# Flask テンプレート差し替え手順

このフォルダの内容を、既存プロジェクトの `templates/web/` にそのままコピー（上書き）してください。

## ファイル構成

```
templates/web/
├── _styles.css       # CSSは {% include %} で読み込まれる（拡張子 .css のままでOK）
├── base.html         # 全画面共通レイアウト（サイドバー/トップバー/ステータスバー）
├── dashboard.html
├── watchlist.html
├── signals.html
├── settings.html
├── login.html        # base.html を継承しないスタンドアロン
└── stub.html         # 仮想売買・ポートフォリオなど未実装ページ用
```

## 既存ルートへの追加変数（任意）

`base.html` の表示を充実させるため、以下を context として渡すと見栄えが良くなります。**渡さなくても動きます**（None なら "—" で表示）。

```python
from datetime import datetime

@app.context_processor
def inject_globals():
    return {
        "now": datetime.now(),
        "market": {
            "n225": "38,420", "n225_pct": -0.82,
            "topix": "2,712", "topix_pct": -0.65,
            "usdjpy": "154.21",
        },
        "counts": {
            "watchlist": db_count_watchlist(),    # int
            "signals": db_count_signals(),        # int (赤バッジで強調)
            "trades": db_count_trades(),          # int
            "portfolio": db_count_portfolio(),    # int
        },
        "alerts": db_count_signals(),  # >0 でベルに赤ドット
    }
```

`now` / `market` / `counts` / `alerts` は全て **未定義でも動く** よう base.html 側でガード済みです。

## ダッシュボードの表示について

`dashboard.html` の `stats` 辞書には `closed` キーを追加すると4つ目のタイルに表示されます。なくても 0 表示。

```python
stats = {
    "watching": ...,
    "rebound_signal": ...,
    "notified": ...,
    "closed": ...,   # 追加（任意）
}
```

## 未実装ページ（仮想売買 / ポートフォリオ）

`stub.html` を使ってください。元と同じ呼び出し方法でOK。

```python
@app.route("/web/virtual-trades")
def virtual_trades():
    return render_template("web/stub.html", title="仮想売買", message="準備中")
```

## Bootstrap は不要

このテンプレは Bootstrap に依存していません。`base.html` から `bootstrap.min.css` / `bootstrap.bundle.min.js` の読み込みは外しています。

## フォント

Google Fonts (Noto Sans JP / JetBrains Mono) を CDN から読み込みます。オフライン環境の場合は self-host してください。
