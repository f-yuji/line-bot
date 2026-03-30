"""
simulate_line.py — handle_message をターミナルから擬似実行するCLIテスト

使い方:
    python simulate_line.py "会話ネタ"
    python simulate_line.py "彼女との会話ネタ" --user-id U1234567890
    python simulate_line.py "使い方"
    python simulate_line.py "停止"
"""

import argparse
import io
import sys

# Windows のコンソールエンコーディングを UTF-8 に統一
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
if sys.stderr.encoding and sys.stderr.encoding.lower() != "utf-8":
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

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

# ─── メイン ───

def main():
    parser = argparse.ArgumentParser(description="LINE handle_message のCLIシミュレーター")
    parser.add_argument("message", help="送信するメッセージ")
    parser.add_argument("--user-id", default="U_simulate_test_user", help="ユーザーID（省略可）")
    args = parser.parse_args()

    _apply_patches()

    import app  # patch後にimportすることで確実に反映
    event = _FakeEvent(user_id=args.user_id, text=args.message)

    print(f"\n{'=' * 40}")
    print(f"  送信: {args.message!r}")
    print(f"  user_id: {args.user_id}")
    print(f"{'=' * 40}")

    try:
        app.handle_message(event)
    except SystemExit:
        pass
    except Exception as e:
        print(f"\n[ERROR] handle_message 内で例外が発生しました:")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    if not _replies:
        print("\n[INFO] 返信なし（分岐途中でreturnされた、またはメッセージ対象外）")
    else:
        for i, reply in enumerate(_replies, 1):
            _print_reply(i, reply)
    print()

if __name__ == "__main__":
    main()
