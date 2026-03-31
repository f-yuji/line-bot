"""
simulate_line.py — handle_message をターミナルから擬似実行するCLIテスト

【単発モード】
    python simulate_line.py "会話ネタ"
    python simulate_line.py "彼女との会話ネタ" --user-id U1234567890

【REPLモード】（引数なし）
    python simulate_line.py
    python simulate_line.py --user-id U1234567890
    → exit / quit で終了
"""

import argparse
import io
import sys

# Windows のコンソールエンコーディングを UTF-8 に統一
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
if sys.stderr.encoding and sys.stderr.encoding.lower() != "utf-8":
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")
if sys.stdin.encoding and sys.stdin.encoding.lower() != "utf-8":
    sys.stdin = io.TextIOWrapper(sys.stdin.buffer, encoding="utf-8", errors="replace")

DEFAULT_USER_ID = "U_simulate_test_user"

# ─── テスト用フィクスチャ ───

class _FakeSource:
    def __init__(self, user_id: str):
        self.user_id = user_id

class _FakeMessage:
    def __init__(self, text: str):
        self.text = text

class _FakeEvent:
    def __init__(self, user_id: str, text: str):
        self.source = _FakeSource(user_id)
        self.message = _FakeMessage(text)
        self.reply_token = "__simulate__"

# ─── monkey patch ───

_replies: list[str] = []

def _patched_reply_text(reply_token, text, quick_reply=None):
    _replies.append(text)

def _patched_reply_flex(reply_token, flex_msg):
    _replies.append(f"[FlexMessage: {getattr(flex_msg, 'alt_text', str(flex_msg))}]")

def _apply_patches():
    import app
    app.reply_text = _patched_reply_text
    app.reply_flex = _patched_reply_flex

# ─── 表示 ───

_SEP = "─" * 40

def _print_reply(index: int, text: str):
    print(f"\n{_SEP}")
    print(f"  返信 [{index}]")
    print(_SEP)
    for line in text.splitlines():
        print(f"  {line}")
    print(_SEP)

# ─── 送信処理（単発・REPL共通） ───

def _send(app_module, user_id: str, text: str):
    """1メッセージを handle_message に通して結果を表示する"""
    _replies.clear()
    event = _FakeEvent(user_id=user_id, text=text)
    try:
        app_module.handle_message(event)
    except SystemExit:
        pass
    except Exception:
        print("\n[ERROR] handle_message 内で例外が発生しました:")
        import traceback
        traceback.print_exc()
        return

    if not _replies:
        print("\n[INFO] 返信なし（分岐途中でreturnされた、またはメッセージ対象外）")
    else:
        for i, reply in enumerate(_replies, 1):
            _print_reply(i, reply)
    print()


# ─── REPLモード ───

def _repl(app_module, user_id: str):
    print(f"LINE シミュレーター起動（user_id: {user_id}）")
    print("exit / quit で終了\n")
    while True:
        try:
            text = input("> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n終了します")
            break
        if text.startswith("> "):
            text = text[2:]
        if not text:
            continue
        if text.lower() in ("exit", "quit"):
            print("終了します")
            break
        print(f"\n{'=' * 40}")
        print(f"  入力: {text}")
        print("─" * 40)
        _send(app_module, user_id, text)


# ─── メイン ───

def main():
    parser = argparse.ArgumentParser(description="LINE handle_message のCLIシミュレーター")
    parser.add_argument("message", nargs="?", default=None, help="送信するメッセージ（省略でREPLモード）")
    parser.add_argument("--user-id", default=DEFAULT_USER_ID, help="ユーザーID（省略可）")
    args = parser.parse_args()

    _apply_patches()
    import app  # patch後にimportすることで確実に反映

    if args.message is None:
        _repl(app, args.user_id)
    else:
        print(f"\n{'=' * 40}")
        print(f"  送信: {args.message!r}")
        print(f"  user_id: {args.user_id}")
        print(f"{'=' * 40}")
        _send(app, args.user_id, args.message)

if __name__ == "__main__":
    main()
