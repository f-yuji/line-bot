import os
import subprocess
import time

import psutil

TARGET_FULL = os.path.abspath(r"C:\bot\line_bot\app.py")
TARGET_NAME = "app.py"
VENV_DIR = r"C:\bot\line_bot\venv"
VENV_SCRIPTS = os.path.join(VENV_DIR, "Scripts")
WORKDIR = r"C:\bot\line_bot"

# venv/Scripts/python.exe は Python Launcher (venvlauncher) のため常に2プロセスになる。
# pyvenv.cfg の executable を直接使い、venv 環境変数を手動セットして1プロセスに保つ。
def _read_base_python():
    cfg = os.path.join(VENV_DIR, "pyvenv.cfg")
    with open(cfg, encoding="utf-8") as f:
        for line in f:
            key, _, val = line.partition("=")
            if key.strip().lower() == "executable":
                return val.strip()
    raise RuntimeError("pyvenv.cfg に executable が見つかりません")

PYTHON_EXE = _read_base_python()

current_pid = os.getpid()
killed = []

for proc in psutil.process_iter(["pid", "name", "cmdline"]):
    try:
        pid = proc.info.get("pid")
        name = (proc.info.get("name") or "").lower()
        cmdline_list = proc.info.get("cmdline") or []

        if pid == current_pid:
            continue

        if "python" not in name:
            continue

        # restart自身は除外
        if any("restart_app.py" in c for c in cmdline_list):
            continue

        # ←ここが重要（配列ベース判定）
        if any(
            TARGET_FULL in c or c.endswith(TARGET_NAME)
            for c in cmdline_list
        ):
            proc.kill()
            killed.append(pid)

    except Exception:
        pass

time.sleep(1)

# venv 環境を手動構築して直接 python312.exe を起動（ランチャー経由なし）
env = os.environ.copy()
env["VIRTUAL_ENV"] = VENV_DIR
env["PATH"] = VENV_SCRIPTS + os.pathsep + env.get("PATH", "")
env.pop("PYTHONHOME", None)

subprocess.Popen(
    [PYTHON_EXE, TARGET_FULL],
    cwd=WORKDIR,
    env=env,
)

print("restarted", killed)