# dev_launcher — 汎用 Python 開発ランチャー

Python プロジェクトの「起動 / 再起動 / 保存監視」を設定ファイル一つで管理する共通ランチャー。  
`launcher_config.json` の値を変えるだけで別プロジェクトにそのまま流用できる。

---

## ファイル構成

```
dev_launcher/
  launcher_config.json   設定ファイル (ここだけ編集する)
  launcher_utils.py      共通ユーティリティ (直接実行しない)
  run_target.py          起動 / 再起動
  restart_target.py      再起動 (VS Code タスク用ラッパー)
  watch_target.py        保存監視 + 自動再起動
  README.md              このファイル
```

---

## 依存パッケージ

```bash
pip install psutil watchdog
```

---

## launcher_config.json 各項目

| キー | 説明 | 例 |
|------|------|-----|
| `project_root` | プロジェクトのルートディレクトリ (絶対パス) | `C:\\Users\\me\\dev\\myapp` |
| `entry_file` | 起動するスクリプト (project_root 相対) | `app.py` |
| `host` | アプリが LISTEN するホスト (ログ表示用) | `0.0.0.0` |
| `port` | アプリが LISTEN するポート (ログ表示用) | `8000` |
| `watch_extensions` | 監視対象の拡張子リスト | `[".py", ".json", ".env"]` |
| `ignore_dirs` | 監視除外ディレクトリ名のリスト | `["venv", ".git", "__pycache__"]` |
| `python_executable` | 使用する python.exe の絶対パス | `C:\\Python312\\python.exe` |
| `use_new_console` | `true` = 別ウィンドウで起動 / `false` = 同ターミナル | `true` |
| `env_overrides` | 子プロセスに追加する環境変数 | `{"DEBUG": "1"}` |

> **注意**: Windows パスはバックスラッシュを `\\` でエスケープする。

---

## 起動方法

```bash
# プロジェクトルートから
python dev_launcher/run_target.py

# 既存プロセスを停止せずに起動したい場合
python dev_launcher/run_target.py --no-kill
```

---

## 再起動方法

```bash
python dev_launcher/restart_target.py
```

または VS Code コマンドパレット → **Tasks: Run Task** → **Restart current target**

---

## 保存監視による自動再起動

```bash
python dev_launcher/watch_target.py

# debounce 秒数を変えたい場合 (デフォルト 1.0 秒)
python dev_launcher/watch_target.py --debounce 2.0
```

または VS Code コマンドパレット → **Tasks: Run Task** → **Watch current target**

`watch_extensions` に一致するファイルを保存するたびに自動で再起動する。  
連続保存時は debounce 秒だけ待機して無限再起動を防ぐ。

---

## branch 表示仕様

起動・再起動のたびに git branch を判定してコンソールに表示する。

| branch | 表示 |
|--------|------|
| `main` | 🔴 main |
| `dev`  | 🟡 dev  |
| その他 | ⚪ branch名 |

子プロセスには以下の環境変数が自動でセットされる:

| 変数名 | 値の例 |
|--------|--------|
| `GIT_BRANCH` | `dev` |
| `GIT_BRANCH_DISPLAY` | `🟡 dev` |

app.py 側で branch を表示したい場合:

```python
import os
branch = os.environ.get("GIT_BRANCH_DISPLAY", "")
if branch:
    print(f"running on {branch}")
```

---

## よくあるハマりポイント

### venv の python.exe で起動するとプロセスが二重に見える

**原因**: Windows + Python 3.12 の `venv/Scripts/python.exe` は launcher stub。  
呼び出すと実際の python.exe を子プロセスとして生成するため PID が2本になる。

**対策**: `python_executable` には venv の python ではなく **system の python.exe** を直接指定する。

```json
"python_executable": "C:\\Users\\f-yuj\\AppData\\Local\\Programs\\Python\\Python312\\python.exe"
```

それでも venv の python を使いたい場合は launcher_utils.py の `resolve_python_exe` が  
`pyvenv.cfg` を読んで実体 python.exe に自動解決する。

---

### psutil が見つからない

```bash
pip install psutil
```

---

### watchdog が見つからない

```bash
pip install watchdog
```

---

### Ctrl+C しても対象プロセスが残る

`use_new_console: true` のとき対象プロセスは別ウィンドウで動くため、  
watch_target.py 終了時に自動 kill される。  
残った場合は `restart_target.py` を実行するか、タスクマネージャーから終了する。

---

### VS Code タスクが python を見つけられない

`.vscode/tasks.json` は `${config:python.defaultInterpreterPath}` を使っている。  
VS Code の Python 拡張でインタープリターを選択していること。  
または tasks.json の `command` を直接 `python_executable` のパスに書き換える。

---

## 別プロジェクトで使い回す手順

1. `dev_launcher/` ディレクトリごとコピーする
2. `.vscode/tasks.json` をコピーする
3. `dev_launcher/launcher_config.json` を新プロジェクト用に編集する  
   (最低限 `project_root` と `entry_file` と `python_executable` を変更)
4. 依存がなければ `pip install psutil watchdog`
5. `python dev_launcher/run_target.py` で起動確認

`launcher_utils.py` / `run_target.py` / `restart_target.py` / `watch_target.py` は  
**一切編集不要**。設定はすべて `launcher_config.json` に集約されている。
