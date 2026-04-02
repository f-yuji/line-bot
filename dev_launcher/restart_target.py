"""
restart_target.py
対象プロセスを明示的に停止して再起動する。
VS Code タスク「Restart current target」から呼ばれることを想定。

使い方:
    python dev_launcher/restart_target.py
"""

from __future__ import annotations

import sys
from pathlib import Path

# dev_launcher ディレクトリを sys.path に追加
sys.path.insert(0, str(Path(__file__).parent))

# run_target の main を再利用 (--no-kill なし = kill してから起動)
from run_target import main

if __name__ == "__main__":
    # sys.argv を run_target に渡すため、restart 専用引数は不要
    # 「再起動」を明示するためだけのラッパー
    print("[restart] === Restart requested ===")
    main()
