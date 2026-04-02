"""
launcher_utils.py
共通ユーティリティ。設定読み込み / git branch / Python解決 / プロセス管理。
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Optional

try:
    import psutil
except ImportError:
    psutil = None  # type: ignore

LAUNCHER_DIR = Path(__file__).parent
CONFIG_FILE = LAUNCHER_DIR / "launcher_config.json"
PID_FILE = LAUNCHER_DIR / ".target.pid"


# ---------------------------------------------------------------------------
# 設定
# ---------------------------------------------------------------------------

def load_config() -> dict:
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# git branch
# ---------------------------------------------------------------------------

def get_git_branch(project_root: str) -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            cwd=project_root,
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode == 0:
            return result.stdout.strip()
    except Exception:
        pass
    return "unknown"


def format_branch(branch: str) -> str:
    if branch == "main":
        return f"\U0001f534 main"      # 🔴
    elif branch == "dev":
        return f"\U0001f7e1 dev"       # 🟡
    else:
        return f"\u26aa {branch}"      # ⚪


def print_branch_banner(project_root: str) -> None:
    branch = get_git_branch(project_root)
    display = format_branch(branch)
    bar = "=" * 50
    print(bar)
    print(f"  Branch : {display}")
    print(f"  Project: {project_root}")
    print(bar)


# ---------------------------------------------------------------------------
# Python 実行ファイル解決 (venv launcher 問題を回避)
# ---------------------------------------------------------------------------

def resolve_python_exe(python_executable: str) -> str:
    """
    venv/Scripts/python.exe は Windows + Python 3.12 では launcher stub になり
    子プロセスを生成するため、プロセスが二重に見える問題がある。
    pyvenv.cfg から base-executable を読み、実体の python.exe を返す。
    設定に直接 system python を書いている場合はそのまま返す。
    """
    exe = Path(python_executable)
    if not exe.exists():
        return python_executable

    # venv の Scripts ディレクトリかチェック
    pyvenv_cfg = exe.parent.parent / "pyvenv.cfg"
    if not pyvenv_cfg.exists():
        return str(exe)

    # pyvenv.cfg を解析して base python を取得
    with open(pyvenv_cfg, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line.lower().startswith("base-executable"):
                _, _, val = line.partition("=")
                base_exe = Path(val.strip())
                if base_exe.exists():
                    return str(base_exe)
            if line.lower().startswith("home"):
                _, _, val = line.partition("=")
                home = Path(val.strip())
                candidate = home / exe.name
                if candidate.exists():
                    return str(candidate)

    return str(exe)


# ---------------------------------------------------------------------------
# PID ファイル管理
# ---------------------------------------------------------------------------

def write_pid(pid: int) -> None:
    PID_FILE.write_text(str(pid), encoding="utf-8")


def read_pid() -> Optional[int]:
    if PID_FILE.exists():
        try:
            return int(PID_FILE.read_text(encoding="utf-8").strip())
        except (ValueError, OSError):
            pass
    return None


def clear_pid() -> None:
    if PID_FILE.exists():
        try:
            PID_FILE.unlink()
        except OSError:
            pass


# ---------------------------------------------------------------------------
# プロセス管理 (psutil 必須)
# ---------------------------------------------------------------------------

def _require_psutil() -> None:
    if psutil is None:
        print("[ERROR] psutil が見つかりません。pip install psutil を実行してください。")
        sys.exit(1)


def find_target_process(entry_file_abs: str, self_pid: int) -> Optional["psutil.Process"]:
    """
    PID ファイル → cmdline スキャン の順で対象プロセスを探す。
    自分自身 (self_pid) と watch_target.py は除外する。
    """
    _require_psutil()

    # 1. PID ファイルで探す
    stored_pid = read_pid()
    if stored_pid and stored_pid != self_pid:
        try:
            proc = psutil.Process(stored_pid)
            if proc.is_running() and proc.status() != psutil.STATUS_ZOMBIE:
                return proc
        except psutil.NoSuchProcess:
            pass
        clear_pid()

    # 2. cmdline スキャン (フォールバック)
    entry_path = Path(entry_file_abs).resolve()
    entry_name = entry_path.name
    launcher_dir = str(LAUNCHER_DIR.resolve())

    for proc in psutil.process_iter(["pid", "name", "cmdline"]):
        if proc.pid == self_pid:
            continue
        try:
            cmdline = proc.cmdline()
            if not cmdline:
                continue
            # dev_launcher 配下のスクリプトは除外 (自分のファミリー)
            if any(launcher_dir in arg for arg in cmdline):
                continue
            # entry_file に一致する引数があるか
            for arg in cmdline[1:]:
                arg_path = Path(arg)
                if arg_path.name != entry_name:
                    continue
                try:
                    if arg_path.resolve() == entry_path:
                        return proc
                except Exception:
                    # パス解決できない場合はファイル名一致で許容
                    return proc
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue

    return None


def kill_target(proc: "psutil.Process") -> bool:
    """対象プロセスとその子プロセスを終了する。"""
    _require_psutil()
    try:
        children = proc.children(recursive=True)
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except psutil.TimeoutExpired:
            proc.kill()
        for child in children:
            try:
                child.kill()
            except psutil.NoSuchProcess:
                pass
        clear_pid()
        return True
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        clear_pid()
        return False


# ---------------------------------------------------------------------------
# プロセス起動
# ---------------------------------------------------------------------------

def build_env(config: dict, branch: str) -> dict:
    env = os.environ.copy()
    # branch 情報を環境変数として渡す
    env["GIT_BRANCH"] = branch
    env["GIT_BRANCH_DISPLAY"] = format_branch(branch)
    # ユーザー指定の上書き
    env.update(config.get("env_overrides", {}))
    return env


def launch_process(config: dict, python_exe: str) -> "subprocess.Popen":
    import subprocess  # noqa: PLC0415

    entry_abs = str(Path(config["project_root"]) / config["entry_file"])
    branch = get_git_branch(config["project_root"])
    env = build_env(config, branch)

    cmd = [python_exe, entry_abs]

    kwargs: dict = {
        "cwd": config["project_root"],
        "env": env,
    }

    use_new_console = config.get("use_new_console", True)
    if use_new_console and sys.platform == "win32":
        kwargs["creationflags"] = subprocess.CREATE_NEW_CONSOLE

    log_dir = Path(config["project_root"]) / "dev_launcher"
    log_dir.mkdir(exist_ok=True)

    log_file = open(log_dir / "target.log", "a", encoding="utf-8")

    kwargs["stdout"] = log_file
    kwargs["stderr"] = log_file

    proc = subprocess.Popen(cmd, **kwargs)
    return proc