import requests
from datetime import datetime
import os

TOKEN = os.environ["LINE_TOKEN"]" \

url = "https://api.line.me/v2/bot/message/broadcast"

headers = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json"
}

today = datetime.now().strftime("%Y-%m-%d")

message = f"""朝レポ {today}

■不動産
- 新着2件
- 当たり1件

■建設
- 原油関連1件

■金利
- 変化なし

■AI
- 自動化系1件
"""

data = {
    "messages": [
        {
            "type": "text",
            "text": message
        }
    ]
}

res = requests.post(url, headers=headers, json=data)
print(res.status_code)
print(res.text)