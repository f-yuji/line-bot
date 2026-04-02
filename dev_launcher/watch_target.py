"""
watch_target.py
ファイル保存を監視して対象プロセスを自動再起動する。
VS Code タスク「Watch current target」から呼ばれることを想定。

依存: watchdog (pip install watchdog)

使い方:
    python dev_launcher/watch_target.py
    python dev_launcher/watch_target.py --debounce 2.0
"""

from __future__ import annotations

import argparse
import os
import sys
import threading
import time
from pathlib import Path

# dev_launcher ディレクトリを sys.path に追加
sys.path.insert(0, str(Path(__file__).parent))

try:
    from watchdog.events import FileSystemEventHandler
    from watchdog.observers import Observer
except ImportError:
    print("[watch] ERROR: watchdog が見つかりません。")
    print("[watch]   pip install watchdog  を実行してください。")
    sys.exit(1)

from launcher_utils import (
    load_config,
    resolve_python_exe,
    print_branch_banner,
    find_target_process,
    kill_target,
    launch_process,
    write_pid,
    LAUNCHER_DIR,
)


class DebounceRestartHandler(FileSystemEventHandler):
    def __init__(
        self,
        config: dict,
        python_exe: str,
        debounce_sec: float = 1.0,
    ) -> None:
        super().__init__()
        self.config = config
        self.python_exe = python_exe
        self.debounce_sec = debounce_sec
        self.watch_extensions = tuple(config.get("watch_extensions", [".py"]))
        self.ignore_dirs = set(config.get("ignore_dirs", []))
        self._timer: threading.Timer | None = None
        self._lock = threading.Lock()

    def _is_watched(self, path: str) -> bool:
        p = Path(path)
        # 拡張子チェック
        if p.suffix not in self.watch_extensions:
            return False
        # 無視ディレクトリチェック (パスの各部分に ignore_dirs が含まれないか)
        parts = set(p.parts)
        if parts & self.ignore_dirs:
            return False
        # dev_launcher 自身のファイルは無視
        try:
            p.resolve().relative_to(LAUNCHER_DIR.resolve())
            return False
        except ValueError:
            pass
        return True

    def on_modified(self, event) -> None:
        if event.is_directory:
            return
        if not self._is_watched(event.src_path):
            return
        rel = Path(event.src_path).name
        print(f"\n[watch] changed: {rel}")
        self._schedule_restart()

    def on_created(self, event) -> None:
        self.on_modified(event)

    def _schedule_restart(self) -> None:
        with self._lock:
            if self._timer is not None:
                self._timer.cancel()
            self._timer = threading.Timer(self.debounce_sec, self._do_restart)
            self._timer.daemon = True
            self._timer.start()

    def _do_restart(self) -> None:
        with self._lock:
            self._timer = None

        config = self.config
        project_root = config["project_root"]
        entry_abs = str(Path(project_root) / config["entry_file"])
        self_pid = os.getpid()

        print("[watch] === restarting target ===")
        print_branch_banner(project_root)

        proc = find_target_process(entry_abs, self_pid)
        if proc is not None:
            print(f"[watch] stopping pid={proc.pid} ...")
            kill_target(proc)
            time.sleep(0.3)
        else:
            print("[watch] no existing process found.")

        child = launch_process(config, self.python_exe)
        write_pid(child.pid)
        print(f"[watch] started pid={child.pid}")

        if not config.get("use_new_console", True):
            # 同一コンソールの場合、プロセスをデタッチして watch を継続
            # (child は daemon 扱いで watch_target が管理しない)
            pass


def main() -> None:
    parser = argparse.ArgumentParser(description="ファイル監視による自動再起動")
    parser.add_argument(
        "--debounce",
        type=float,
        default=1.0,
        help="連続保存時の再起動抑制秒数 (デフォルト: 1.0)",
    )
    args = parser.parse_args()

    config = load_config()
    project_root = config["project_root"]
    entry_abs = str(Path(project_root) / config["entry_file"])

    print_branch_banner(project_root)
    print(f"[watch] target     : {entry_abs}")
    print(f"[watch] extensions : {config.get('watch_extensions', [])}")
    print(f"[watch] ignore_dirs: {config.get('ignore_dirs', [])}")
    print(f"[watch] debounce   : {args.debounce}s")

    raw_exe = config.get("python_executable", sys.executable)
    python_exe = resolve_python_exe(raw_exe)
    if python_exe != raw_exe:
        print(f"[watch] python     : {raw_exe} -> {python_exe} (venv resolved)")
    else:
        print(f"[watch] python     : {python_exe}")

    # 初回起動
    print(f"[watch] initial start ...")
    self_pid = os.getpid()
    proc = find_target_process(entry_abs, self_pid)
    if proc is not None:
        print(f"[watch] found running pid={proc.pid}, stopping ...")
        kill_target(proc)
        time.sleep(0.3)
    child = launch_process(config, python_exe)
    write_pid(child.pid)
    print(f"[watch] started pid={child.pid}")
    print(f"[watch] watching {project_root} ...")
    print("[watch] Ctrl+C to stop watching.\n")

    # watchdog セットアップ
    handler = DebounceRestartHandler(config, python_exe, debounce_sec=args.debounce)
    observer = Observer()
    observer.schedule(handler, project_root, recursive=True)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[watch] stopping observer ...")
        observer.stop()
        # 対象プロセスも終了
        proc = find_target_process(entry_abs, self_pid)
        if proc is not None:
            print(f"[watch] stopping target pid={proc.pid} ...")
            kill_target(proc)
    observer.join()
    print("[watch] done.")


if __name__ == "__main__":
    main()
