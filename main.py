import os, json, re
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

import httpx
import pandas as pd
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, PlainTextResponse, FileResponse, RedirectResponse
from dotenv import load_dotenv

load_dotenv()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
ADMIN_KEY = os.getenv("ADMIN_KEY", "")
DATA_DIR = os.getenv("DATA_DIR", ".")  # Render: /var/data
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").rstrip("/")
WEBHOOK_PATH_SECRET = os.getenv("WEBHOOK_PATH_SECRET", "hook")
WEBHOOK_SECRET_TOKEN = os.getenv("WEBHOOK_SECRET_TOKEN", "")

if not TELEGRAM_TOKEN:
    raise RuntimeError("Missing TELEGRAM_TOKEN")

os.makedirs(DATA_DIR, exist_ok=True)
DATA_FILE = os.path.join(DATA_DIR, "data.json")

# ---------------- Data ----------------
def load_db() -> Dict[str, Any]:
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {"chats": {}}

def save_db():
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(DB, f, ensure_ascii=False, indent=2)

def get_chat(chat_id: int) -> Dict[str, Any]:
    cid = str(chat_id)
    chats = DB.setdefault("chats", {})
    if cid not in chats:
        chats[cid] = {"front": 0.0, "manual": 0.0, "ret": 0.0, "logs": []}
        save_db()
    for k in ["front", "manual", "ret", "logs"]:
        if k not in chats[cid]:
            chats[cid][k] = 0.0 if k != "logs" else []
    return chats[cid]

def add_log(chat_id: int, chat_name: str, user: str, kind: str, amount: float):
    chat = get_chat(chat_id)
    chat["logs"].append({
        "time": datetime.now().isoformat(timespec="seconds"),
        "user": user,
        "kind": kind,     # 前數 / 手動 / 回數 / 重置
        "amount": amount,
        "chat_id": chat_id,
        "chat_name": chat_name,
    })
    save_db()

def parse_iso(t: str) -> Optional[datetime]:
    try:
        return datetime.fromisoformat(t)
    except Exception:
        return None

DB = load_db()

# ---------------- Telegram API helpers ----------------
TG_API = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"

async def tg_send_message(chat_id: int, text: str):
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.post(f"{TG_API}/sendMessage", json={
            "chat_id": chat_id,
            "text": text
        })
        data = r.json()
        if not data.get("ok"):
            raise RuntimeError(str(data))

# ---------------- Excel export (含餘額) ----------------
def export_excel(chat_id: int, start_dt: datetime, end_dt: datetime) -> Optional[str]:
    chat = get_chat(chat_id)

    parsed: List[tuple[datetime, Dict[str, Any]]] = []
    for log in chat.get("logs", []):
        t = parse_iso(log.get("time", ""))
        if t:
            parsed.append((t, log))
    parsed.sort(key=lambda x: x[0])

    running_front = 0.0
    running_manual = 0.0
    running_ret = 0.0

    rows = []
    for t, log in parsed:
        kind = str(log.get("kind", ""))
        amount = float(log.get("amount", 0.0))

        if kind == "前數":
            running_front += amount
        elif kind == "手動":
            running_manual += amount
        elif kind == "回數":
            running_ret += amount
        elif kind == "重置":
            running_front = running_manual = running_ret = 0.0

        balance = running_front + running_manual - running_ret

        if start_dt <= t <= end_dt:
            rows.append({
                "時間": t.strftime("%Y-%m-%d %H:%M:%S"),
                "操作人": log.get("user", ""),
                "類型": kind,
                "數值": amount,
                "群組/私聊": log.get("chat_name", ""),
                "餘額": balance,
            })

    if not rows:
        return None

    df = pd.DataFrame(rows)
    fn = datetime.now().strftime(f"export_{chat_id}_%Y%m%d_%H%M%S.xlsx")
    path = os.path.join(DATA_DIR, fn)
    df.to_excel(path, index=False)
    return path

def require_admin(key: str):
    if not ADMIN_KEY or key != ADMIN_KEY:
        raise HTTPException(status_code=403, detail="Forbidden")

# ---------------- FastAPI ----------------
app = FastAPI()

@app.get("/", response_class=PlainTextResponse)
async def root():
    return "OK"

@app.post(f"/telegram/{WEBHOOK_PATH_SECRET}")
async def telegram_webhook(request: Request):
    # 1) 驗證 Telegram secret token header（如果你有設定）
    if WEBHOOK_SECRET_TOKEN:
        got = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
        if got != WEBHOOK_SECRET_TOKEN:
            raise HTTPException(status_code=403, detail="Bad secret token")

    payload = await request.json()

    msg = payload.get("message") or payload.get("edited_message")
    if not msg:
        return {"ok": True}

    chat = msg.get("chat", {})
    chat_id = int(chat.get("id"))
    chat_name = chat.get("title") or "Private"

    user_obj = msg.get("from", {}) or {}
    user = user_obj.get("username") or user_obj.get("first_name") or str(user_obj.get("id"))

    text = (msg.get("text") or "").strip()
    if not text:
        return {"ok": True}

    st = get_chat(chat_id)

    # 指令解析
    def parse_amount(prefix: str) -> Optional[float]:
        try:
            return float(text[len(prefix):].strip())
        except Exception:
            return None

    if text.startswith("前="):
        amount = parse_amount("前=")
        if amount is None:
            await tg_send_message(chat_id, "❌ 格式：前=100")
            return {"ok": True}
        st["front"] += amount
        add_log(chat_id, chat_name, user, "前數", amount)
        await tg_send_message(chat_id, f"✅ 前數={st['front']}")
        return {"ok": True}

    if text.startswith("手="):
        amount = parse_amount("手=")
        if amount is None:
            await tg_send_message(chat_id, "❌ 格式：手=100")
            return {"ok": True}
        st["manual"] += amount
        add_log(chat_id, chat_name, user, "手動", amount)
        await tg_send_message(chat_id, f"✅ 手動={st['manual']}")
        return {"ok": True}

    if text.startswith("回="):
        amount = parse_amount("回=")
        if amount is None:
            await tg_send_message(chat_id, "❌ 格式：回=100")
            return {"ok": True}
        st["ret"] += amount
        add_log(chat_id, chat_name, user, "回數", amount)
        await tg_send_message(chat_id, f"✅ 回數={st['ret']}")
        return {"ok": True}

    if text == "總計":
        balance = st["front"] + st["manual"] - st["ret"]
        await tg_send_message(
            chat_id,
            f"📊 本群統計\n前數：{st['front']}\n手動：{st['manual']}\n回數：{st['ret']}\n—\n💰 餘額：{balance}"
        )
        return {"ok": True}

    if text == "清空":
        st["front"] = st["manual"] = st["ret"] = 0.0
        add_log(chat_id, chat_name, user, "重置", 0.0)
        save_db()
        await tg_send_message(chat_id, "🧹 已清空（前數/手動/回數）")
        return {"ok": True}

    if text == "匯出":
        if not PUBLIC_BASE_URL:
            await tg_send_message(chat_id, "⚠️ 尚未設定 PUBLIC_BASE_URL（請到 Render Environment 填入你的網址）")
            return {"ok": True}

        end_dt = datetime.now()
        start_dt = end_dt - timedelta(days=30)
        url = (
            f"{PUBLIC_BASE_URL}/admin/export?"
            f"key={ADMIN_KEY}&chat_id={chat_id}"
            f"&start={start_dt.strftime('%Y-%m-%d %H:%M:%S')}"
            f"&end={end_dt.strftime('%Y-%m-%d %H:%M:%S')}"
        )
        await tg_send_message(chat_id, f"📄 下載 Excel（含餘額）：\n{url}")
        return {"ok": True}

    return {"ok": True}

@app.get("/admin", response_class=HTMLResponse)
async def admin_home(key: str):
    require_admin(key)
    chats = DB.get("chats", {})
    options = []
    for cid, obj in chats.items():
        name = obj.get("logs", [{}])[-1].get("chat_name", cid) if obj.get("logs") else cid
        options.append(f"<option value='{cid}'>{cid} - {name}</option>")

    html = f"""
    <html><head><meta charset="utf-8"><title>Telegram 記帳後台</title></head>
    <body style="font-family:Arial; padding:20px;">
      <h2>Telegram 記帳後台</h2>
      <p>資料檔：<code>{DATA_FILE}</code></p>

      <p><a href="/setup-webhook?key={key}">✅ 設定 Webhook（首次部署點一次）</a></p>

      <h3>下載 Excel（含餘額）</h3>
      <form method="get" action="/admin/export">
        <input type="hidden" name="key" value="{key}">
        <div>
          <label>Chat：</label>
          <select name="chat_id" required>
            {''.join(options)}
          </select>
        </div>
        <div style="margin-top:10px;">
          <label>開始（YYYY-MM-DD HH:MM:SS）：</label>
          <input name="start" style="width:220px;" placeholder="2026-02-01 00:00:00" required>
        </div>
        <div style="margin-top:10px;">
          <label>結束（YYYY-MM-DD HH:MM:SS）：</label>
          <input name="end" style="width:220px;" placeholder="2026-02-27 23:59:59" required>
        </div>
        <div style="margin-top:15px;">
          <button type="submit">下載</button>
        </div>
      </form>
    </body></html>
    """
    return HTMLResponse(html)

@app.get("/admin/export")
async def admin_export(key: str, chat_id: int, start: str, end: str):
    require_admin(key)
    try:
        start_dt = datetime.strptime(start, "%Y-%m-%d %H:%M:%S")
        end_dt = datetime.strptime(end, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        raise HTTPException(status_code=400, detail="Bad datetime format")

    path = export_excel(chat_id, start_dt, end_dt)
    if not path:
        raise HTTPException(status_code=404, detail="No data in range")
    return FileResponse(path, filename=os.path.basename(path))

@app.get("/setup-webhook")
async def setup_webhook(key: str):
    require_admin(key)
    if not PUBLIC_BASE_URL:
        raise HTTPException(status_code=400, detail="Missing PUBLIC_BASE_URL")

    webhook_url = f"{PUBLIC_BASE_URL}/telegram/{WEBHOOK_PATH_SECRET}"
    payload = {"url": webhook_url}
    if WEBHOOK_SECRET_TOKEN:
        payload["secret_token"] = WEBHOOK_SECRET_TOKEN

    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.post(f"{TG_API}/setWebhook", json=payload)
        data = r.json()
        if not data.get("ok"):
            raise HTTPException(status_code=500, detail=str(data))

    return RedirectResponse(url=f"/admin?key={key}")
