"""
run_target.py
対象プロセスを起動 (既に起動中なら停止してから再起動) するメインランチャー。

使い方:
    python dev_launcher/run_target.py
    python dev_launcher/run_target.py --no-kill   # 既存プロセスを止めずに起動
"""

from __future__ import annotations

import os
import sys
import time
import argparse
from pathlib import Path

# dev_launcher ディレクトリを sys.path に追加
sys.path.insert(0, str(Path(__file__).parent))

from launcher_utils import (
    load_config,
    resolve_python_exe,
    print_branch_banner,
    find_target_process,
    kill_target,
    launch_process,
    write_pid,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="対象 Python アプリを起動/再起動する")
    parser.add_argument(
        "--no-kill",
        action="store_true",
        help="既存プロセスを停止せずに起動する",
    )
    args = parser.parse_args()

    config = load_config()
    project_root = config["project_root"]
    entry_file = config["entry_file"]
    entry_abs = str(Path(project_root) / entry_file)
    self_pid = os.getpid()

    print_branch_banner(project_root)
    print(f"[launcher] target : {entry_abs}")

    # Python 実行ファイルを解決 (venv launcher 問題を回避)
    raw_exe = config.get("python_executable", sys.executable)
    python_exe = resolve_python_exe(raw_exe)
    if python_exe != raw_exe:
        print(f"[launcher] python  : {raw_exe}")
        print(f"[launcher]        -> {python_exe} (venv から解決)")
    else:
        print(f"[launcher] python  : {python_exe}")

    # 既存プロセスを停止
    if not args.no_kill:
        proc = find_target_process(entry_abs, self_pid)
        if proc is not None:
            print(f"[launcher] stopping pid={proc.pid} ...")
            killed = kill_target(proc)
            if killed:
                print(f"[launcher] stopped.")
            else:
                print(f"[launcher] already gone.")
            time.sleep(0.3)  # ポート解放待ち
        else:
            print("[launcher] no existing process found.")

    # 起動
    print(f"[launcher] starting {entry_file} ...")
    child = launch_process(config, python_exe)
    write_pid(child.pid)
    print(f"[launcher] started  pid={child.pid}")
    print(f"[launcher] use_new_console={config.get('use_new_console', True)}")

    if not config.get("use_new_console", True):
        # 同一ターミナルで動かす場合は子プロセスの終了を待つ
        print("[launcher] running in this terminal. Ctrl+C to stop.")
        try:
            child.wait()
        except KeyboardInterrupt:
            print("\n[launcher] interrupted. stopping ...")
            kill_target(child)


if __name__ == "__main__":
    main()
