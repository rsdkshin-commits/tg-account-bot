import os, json
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

import httpx
import pandas as pd
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, PlainTextResponse, FileResponse, RedirectResponse

from telegram import Update
from telegram.ext import Application, MessageHandler, ContextTypes, filters
from dotenv import load_dotenv

load_dotenv()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
ADMIN_KEY = os.getenv("ADMIN_KEY", "")  # 後台密鑰
DATA_DIR = os.getenv("DATA_DIR", ".")   # Render: /var/data
WEBHOOK_PATH_SECRET = os.getenv("WEBHOOK_PATH_SECRET", "hook")  # Webhook 路徑秘密字串
WEBHOOK_SECRET_TOKEN = os.getenv("WEBHOOK_SECRET_TOKEN", "")    # Telegram header 驗證
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "")              # 例如 https://xxx.onrender.com

os.makedirs(DATA_DIR, exist_ok=True)
DATA_FILE = os.path.join(DATA_DIR, "data.json")

# ----------------- Data -----------------
def _load_data() -> Dict[str, Any]:
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {"chats": {}}

def _save_data():
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(DB, f, ensure_ascii=False, indent=2)

def get_chat(chat_id: int) -> Dict[str, Any]:
    cid = str(chat_id)
    chats = DB.setdefault("chats", {})
    if cid not in chats:
        chats[cid] = {
            "front_total": 0.0,   # 前數
            "manual_total": 0.0,  # 手動
            "return_total": 0.0,  # 回數
            "logs": [],
        }
        _save_data()
    # 補欄位
    for k in ["front_total", "manual_total", "return_total", "logs"]:
        if k not in chats[cid]:
            chats[cid][k] = 0.0 if k != "logs" else []
    return chats[cid]

def add_log(chat_id: int, chat_name: str, user: str, kind: str, amount: float):
    chat = get_chat(chat_id)
    chat["logs"].append({
        "time": datetime.now().isoformat(timespec="seconds"),
        "user": user,
        "kind": kind,      # 前數/手動/回數/重置
        "amount": amount,
        "chat_id": chat_id,
        "chat_name": chat_name,
    })
    _save_data()

def parse_iso(t: str) -> Optional[datetime]:
    try:
        return datetime.fromisoformat(t)
    except Exception:
        return None

DB = _load_data()

# ----------------- Excel (含餘額) -----------------
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
    running_return = 0.0

    rows = []
    for t, log in parsed:
        kind = str(log.get("kind", ""))
        amount = float(log.get("amount", 0.0))

        if kind == "前數":
            running_front += amount
        elif kind == "手動":
            running_manual += amount
        elif kind == "回數":
            running_return += amount
        elif kind == "重置":
            running_front = running_manual = running_return = 0.0

        balance = running_front + running_manual - running_return

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

# ----------------- Telegram handlers -----------------
async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return

    text = update.message.text.strip()
    chat_id = update.effective_chat.id
    chat_name = update.effective_chat.title or "Private"
    u = update.effective_user
    user = (u.username or u.full_name or str(u.id))

    chat = get_chat(chat_id)

    def reply(s: str):
        return update.message.reply_text(s)

    # 前= → 前數
    if text.startswith("前="):
        try:
            amount = float(text[2:].strip())
        except ValueError:
            await reply("❌ 格式：前=100")
            return
        chat["front_total"] += amount
        add_log(chat_id, chat_name, user, "前數", amount)
        await reply(f"✅ 前數={chat['front_total']}")
        return

    # 手= → 手動
    if text.startswith("手="):
        try:
            amount = float(text[2:].strip())
        except ValueError:
            await reply("❌ 格式：手=100")
            return
        chat["manual_total"] += amount
        add_log(chat_id, chat_name, user, "手動", amount)
        await reply(f"✅ 手動={chat['manual_total']}")
        return

    # 回= → 回數
    if text.startswith("回="):
        try:
            amount = float(text[2:].strip())
        except ValueError:
            await reply("❌ 格式：回=100")
            return
        chat["return_total"] += amount
        add_log(chat_id, chat_name, user, "回數", amount)
        await reply(f"✅ 回數={chat['return_total']}")
        return

    if text == "總計":
        front = chat["front_total"]
        manual = chat["manual_total"]
        ret = chat["return_total"]
        balance = front + manual - ret
        await reply(
            f"📊 本群統計\n"
            f"前數：{front}\n"
            f"手動：{manual}\n"
            f"回數：{ret}\n"
            f"—\n"
            f"💰 餘額：{balance}"
        )
        return

    if text == "清空":
        chat["front_total"] = 0.0
        chat["manual_total"] = 0.0
        chat["return_total"] = 0.0
        add_log(chat_id, chat_name, user, "重置", 0.0)
        _save_data()
        await reply("🧹 已清空（前數/手動/回數）")
        return

    if text == "匯出":
        # 用後台連結下載，避免 Telegram 互動按鈕複雜
        base = PUBLIC_BASE_URL.rstrip("/")
        if not base:
            await reply("⚠️ 後台網址尚未設定（請設定 PUBLIC_BASE_URL）")
            return

        # 預設給你「過去30天」
        end_dt = datetime.now()
        start_dt = end_dt - timedelta(days=30)

        url = (f"{base}/admin/export?"
               f"key={ADMIN_KEY}&chat_id={chat_id}"
               f"&start={start_dt.strftime('%Y-%m-%d %H:%M:%S')}"
               f"&end={end_dt.strftime('%Y-%m-%d %H:%M:%S')}")
        await reply(f"📄 下載 Excel（含餘額）：\n{url}")
        return

# ----------------- FastAPI App + Telegram webhook -----------------
app = FastAPI()
tg_app: Optional[Application] = None

@app.on_event("startup")
async def startup():
    global tg_app
    if not TELEGRAM_TOKEN:
        raise RuntimeError("Missing TELEGRAM_TOKEN")
    tg_app = Application.builder().token(TELEGRAM_TOKEN).build()
    tg_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))
    tg_app.add_handler(MessageHandler(filters.TEXT & filters.COMMAND, on_text))  # 允許用戶打 /總計 也會進來（你可自行刪）
    await tg_app.initialize()
    await tg_app.start()

@app.on_event("shutdown")
async def shutdown():
    global tg_app
    if tg_app:
        await tg_app.stop()
        await tg_app.shutdown()

@app.get("/", response_class=PlainTextResponse)
async def root():
    return "OK"

@app.post(f"/telegram/{WEBHOOK_PATH_SECRET}")
async def telegram_webhook(request: Request):
    # Telegram 官方 secret_token header 驗證 :contentReference[oaicite:2]{index=2}
    if WEBHOOK_SECRET_TOKEN:
        got = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
        if got != WEBHOOK_SECRET_TOKEN:
            raise HTTPException(status_code=403, detail="Bad secret token")

    payload = await request.json()
    update = Update.de_json(payload, tg_app.bot)  # type: ignore
    await tg_app.process_update(update)  # type: ignore
    return {"ok": True}

@app.get("/admin", response_class=HTMLResponse)
async def admin_home(key: str, chat_id: Optional[int] = None):
    require_admin(key)
    # 列出 chats
    chats = DB.get("chats", {})
    options = []
    for cid, obj in chats.items():
        name = obj.get("logs", [{}])[-1].get("chat_name", cid) if obj.get("logs") else cid
        sel = "selected" if (chat_id is not None and str(chat_id) == cid) else ""
        options.append(f"<option value='{cid}' {sel}>{cid} - {name}</option>")

    base = PUBLIC_BASE_URL.rstrip("/")
    html = f"""
    <html><head><meta charset="utf-8"><title>Telegram 記帳後台</title></head>
    <body style="font-family:Arial; padding:20px;">
      <h2>Telegram 記帳後台</h2>
      <p>資料檔：<code>{DATA_FILE}</code></p>

      <form method="get" action="/admin/export">
        <input type="hidden" name="key" value="{key}">
        <div>
          <label>Chat：</label>
          <select name="chat_id" required>
            {''.join(options)}
          </select>
        </div>
        <div style="margin-top:10px;">
          <label>開始(YYYY-MM-DD HH:MM:SS)：</label>
          <input name="start" style="width:220px;" placeholder="2026-02-01 00:00:00" required>
        </div>
        <div style="margin-top:10px;">
          <label>結束(YYYY-MM-DD HH:MM:SS)：</label>
          <input name="end" style="width:220px;" placeholder="2026-02-25 23:59:59" required>
        </div>
        <div style="margin-top:15px;">
          <button type="submit">下載 Excel（含餘額）</button>
        </div>
      </form>

      <hr/>
      <p>快捷：</p>
      <ul>
        <li><a href="{base}/setup-webhook?key={key}">設定 Webhook（首次部署要點一次）</a></li>
      </ul>
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

    webhook_url = f"{PUBLIC_BASE_URL.rstrip('/')}/telegram/{WEBHOOK_PATH_SECRET}"

    # 呼叫 Telegram setWebhook（可帶 secret_token）:contentReference[oaicite:3]{index=3}
    payload = {"url": webhook_url}
    if WEBHOOK_SECRET_TOKEN:
        payload["secret_token"] = WEBHOOK_SECRET_TOKEN

    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/setWebhook",
            json=payload
        )
        data = r.json()
        if not data.get("ok"):
            raise HTTPException(status_code=500, detail=str(data))
    return RedirectResponse(url=f"/admin?key={key}")