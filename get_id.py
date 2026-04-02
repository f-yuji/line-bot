from flask import Flask, request
import json

app = Flask(__name__)

@app.route("/callback", methods=["POST"])
def callback():
    print("==== WEBHOOK HIT ====")
    print("RAW:", request.get_data(as_text=True))

    body = request.get_json(silent=True)
    if body:
        print(json.dumps(body, indent=2, ensure_ascii=False))
    else:
        print("NO JSON")

    return "OK", 200

app.run(host="0.0.0.0", port=5000)