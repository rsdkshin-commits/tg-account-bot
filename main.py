import os
import json
import asyncio
import traceback
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional, Tuple

import httpx
import pandas as pd
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, PlainTextResponse, FileResponse, RedirectResponse, JSONResponse
from dotenv import load_dotenv

load_dotenv()

# ---------------- Config ----------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
ADMIN_KEY = os.getenv("ADMIN_KEY", "").strip()

# Render Disk 建議掛載到 /var/data
DATA_DIR = os.getenv("DATA_DIR", "/var/data").strip()
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").strip().rstrip("/")
WEBHOOK_PATH_SECRET = os.getenv("WEBHOOK_PATH_SECRET", "hook").strip()
WEBHOOK_SECRET_TOKEN = os.getenv("WEBHOOK_SECRET_TOKEN", "").strip()

# 時區：UTC+8（台灣/港/新）
UTC8 = timezone(timedelta(hours=8))

if not TELEGRAM_TOKEN:
    raise RuntimeError("Missing TELEGRAM_TOKEN")

os.makedirs(DATA_DIR, exist_ok=True)
DATA_FILE = os.path.join(DATA_DIR, "data.json")

TG_API = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"


# ---------------- Data (atomic write + corrupt backup) ----------------
def load_db() -> Dict[str, Any]:
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                if not isinstance(data, dict):
                    raise ValueError("DB is not dict")
                return data
        except Exception:
            # JSON 壞了就先備份，不要直接覆蓋
            try:
                bad_name = DATA_FILE + ".corrupt." + datetime.now(UTC8).strftime("%Y%m%d_%H%M%S")
                os.replace(DATA_FILE, bad_name)
            except Exception:
                pass
    return {"chats": {}, "seen_msgs": {}, "meta": {}}


DB: Dict[str, Any] = load_db()


def save_db():
    # 原子寫入：避免重啟/中斷造成 data.json 半截 -> 歸零
    tmp = DATA_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(DB, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, DATA_FILE)


def get_chat(chat_id: int) -> Dict[str, Any]:
    cid = str(chat_id)
    chats = DB.setdefault("chats", {})
    if cid not in chats:
        chats[cid] = {"front": 0.0, "manual": 0.0, "ret": 0.0, "logs": []}
        save_db()
    # 補欄位
    for k in ["front", "manual", "ret", "logs"]:
        if k not in chats[cid]:
            chats[cid][k] = 0.0 if k != "logs" else []
    return chats[cid]


def parse_iso_any(t: str) -> Optional[datetime]:
    try:
        dt = datetime.fromisoformat(t)
        # 舊資料若沒 tzinfo，當作 UTC+8
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC8)
        return dt
    except Exception:
        return None


def add_log(chat_id: int, chat_name: str, user: str, kind: str, amount: float):
    st = get_chat(chat_id)
    st["logs"].append(
        {
            "time": datetime.now(UTC8).isoformat(timespec="seconds"),
            "user": user,
            "kind": kind,  # 前數 / 手動 / 回數 / 清空
            "amount": round(float(amount), 2),
            "chat_id": chat_id,
            "chat_name": chat_name,
        }
    )
    save_db()


# ---------------- Helpers ----------------
def fmt2(x: float) -> str:
    return f"{float(x):.2f}"


def require_admin(key: str):
    if not ADMIN_KEY or key != ADMIN_KEY:
        raise HTTPException(status_code=403, detail="Forbidden")


def parse_dt_to_utc8(s: str) -> datetime:
    """
    支援：
    1) YYYY-MM-DD HH:MM:SS
    2) YYYY-MM-DDTHH:MM
    3) YYYY-MM-DDTHH:MM:SS   (datetime-local step=1)
    回傳 tz-aware (UTC+8)
    """
    s = (s or "").strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%dT%H:%M:%S"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.replace(tzinfo=UTC8)
        except ValueError:
            pass
    raise ValueError("Bad datetime format")


async def tg_send_message(chat_id: int, text: str):
    """
    重點：
    - 不要 raise（避免 webhook 500）
    - 連線抖動時重試
    """
    payload = {"chat_id": chat_id, "text": text}
    delays = [0.0, 0.5, 1.0, 2.0]  # 4 次

    for i, d in enumerate(delays):
        if d:
            await asyncio.sleep(d)
        try:
            timeout = httpx.Timeout(10.0, connect=5.0)
            async with httpx.AsyncClient(timeout=timeout) as client:
                r = await client.post(f"{TG_API}/sendMessage", json=payload)
                data = r.json()
                if data.get("ok"):
                    return
                # Telegram 回 not ok 也不要炸 webhook
                print("sendMessage not ok:", data)
                return
        except Exception as e:
            # 最後一次才印
            if i == len(delays) - 1:
                print("sendMessage failed:", repr(e))
            continue
    return


def record_last_error(err: Exception):
    """把最近一次 webhook 錯誤存進 DB，方便 /debug_last_error 查"""
    tb = traceback.format_exc()
    meta = DB.setdefault("meta", {})
    meta["last_webhook_error"] = {
        "time": datetime.now(UTC8).isoformat(timespec="seconds"),
        "error": repr(err),
        # traceback 太長會很難看，存前 60 行夠用
        "traceback": "\n".join(tb.splitlines()[:60]),
    }
    try:
        save_db()
    except Exception:
        # 就算寫檔也失敗，也不要讓 webhook 500
        pass


# ---------------- Excel export (含餘額) ----------------
def export_excel(chat_id: int, start_dt: datetime, end_dt: datetime) -> Optional[str]:
    st = get_chat(chat_id)

    parsed: List[Tuple[datetime, Dict[str, Any]]] = []
    for log in st.get("logs", []):
        t = parse_iso_any(log.get("time", ""))
        if t:
            parsed.append((t, log))
    parsed.sort(key=lambda x: x[0])

    running_front = 0.0
    running_manual = 0.0
    running_ret = 0.0

    rows = []
    for t, log in parsed:
        kind = str(log.get("kind", ""))
        amount = round(float(log.get("amount", 0.0)), 2)

        if kind == "前數":
            running_front = round(running_front + amount, 2)
        elif kind == "手動":
            running_manual = round(running_manual + amount, 2)
        elif kind == "回數":
            running_ret = round(running_ret + amount, 2)
        elif kind == "清空":
            running_front = running_manual = running_ret = 0.0

        balance = round(running_front + running_manual - running_ret, 2)

        if start_dt <= t <= end_dt:
            rows.append(
                {
                    "時間": t.astimezone(UTC8).strftime("%Y-%m-%d %H:%M:%S"),
                    "操作人": log.get("user", ""),
                    "類型": kind,
                    "數值": amount,
                    "群組/私聊": log.get("chat_name", ""),
                    "餘額": balance,
                }
            )

    if not rows:
        return None

    df = pd.DataFrame(rows)
    fn = datetime.now(UTC8).strftime(f"export_{chat_id}_%Y%m%d_%H%M%S.xlsx")
    path = os.path.join(DATA_DIR, fn)
    df.to_excel(path, index=False)
    return path


# ---------------- FastAPI ----------------
app = FastAPI()


@app.get("/", response_class=PlainTextResponse)
async def root():
    return "OK"


@app.get("/health", response_class=JSONResponse)
async def health():
    """
    Render/UptimeRobot 用：
    - 200 代表服務活著
    - 會回基本狀態（不含敏感資訊）
    """
    chats = DB.get("chats", {}) or {}
    meta = DB.get("meta", {}) or {}
    last_err = meta.get("last_webhook_error")
    return {
        "ok": True,
        "time_utc8": datetime.now(UTC8).isoformat(timespec="seconds"),
        "data_file": DATA_FILE,
        "chats_count": len(chats),
        "has_last_error": bool(last_err),
        "last_error_time": (last_err or {}).get("time"),
    }


@app.get("/debug_last_error", response_class=PlainTextResponse)
async def debug_last_error(key: str):
    """
    用 ADMIN_KEY 查看最近一次 webhook 例外（不用翻 logs）
    """
    require_admin(key)
    meta = DB.get("meta", {}) or {}
    last_err = meta.get("last_webhook_error")
    if not last_err:
        return "✅ No webhook error recorded."

    return (
        "🚨 Last webhook error\n"
        f"Time (UTC+8): {last_err.get('time')}\n"
        f"Error: {last_err.get('error')}\n\n"
        f"{last_err.get('traceback')}\n"
    )


def _dedupe_by_message(chat_id: int, message_id: int) -> bool:
    """
    True = 已處理過（要忽略）
    False = 沒處理過（可處理並記錄）
    """
    if not message_id:
        return False

    seen: Dict[str, str] = DB.setdefault("seen_msgs", {})
    k = f"{chat_id}:{message_id}"
    if k in seen:
        return True

    seen[k] = datetime.now(UTC8).isoformat(timespec="seconds")

    # 控制大小，避免無限增長
    if len(seen) > 10000:
        for kk in list(seen.keys())[:2000]:
            seen.pop(kk, None)

    save_db()
    return False


@app.post(f"/telegram/{WEBHOOK_PATH_SECRET}")
async def telegram_webhook(request: Request):
    # ✅ 永遠不要讓 webhook 回 500（避免漏收/重送/重複）
    try:
        # Verify Telegram secret token header if set
        if WEBHOOK_SECRET_TOKEN:
            got = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
            if got != WEBHOOK_SECRET_TOKEN:
                raise HTTPException(status_code=403, detail="Bad secret token")

        payload = await request.json()

        # 只處理 message（避免 edited_message 造成重算/亂序）
        msg = payload.get("message")
        if not msg:
            return {"ok": True}

        chat = msg.get("chat", {}) or {}
        chat_id = int(chat.get("id"))
        chat_name = chat.get("title") or "Private"

        message_id = int(msg.get("message_id") or 0)
        if _dedupe_by_message(chat_id, message_id):
            return {"ok": True}

        user_obj = msg.get("from", {}) or {}
        user = user_obj.get("username") or user_obj.get("first_name") or str(user_obj.get("id"))

        # ✅ 同時讀 text + caption（照片/文件附文字也能算）
        text = (msg.get("text") or msg.get("caption") or "").strip()
        if not text:
            return {"ok": True}

        st = get_chat(chat_id)

        def parse_amount(prefix: str) -> Optional[float]:
            try:
                return float(text[len(prefix) :].strip())
            except Exception:
                return None

        # 前=
        if text.startswith("前="):
            amount = parse_amount("前=")
            if amount is None:
                await tg_send_message(chat_id, "❌ 格式：前=100")
                return {"ok": True}

            st["front"] = round(float(st["front"]) + float(amount), 2)
            add_log(chat_id, chat_name, user, "前數", amount)
            await tg_send_message(chat_id, f"✅ 前數={fmt2(st['front'])}")
            return {"ok": True}

        # 手=
        if text.startswith("手="):
            amount = parse_amount("手=")
            if amount is None:
                await tg_send_message(chat_id, "❌ 格式：手=100")
                return {"ok": True}

            st["manual"] = round(float(st["manual"]) + float(amount), 2)
            add_log(chat_id, chat_name, user, "手動", amount)
            await tg_send_message(chat_id, f"✅ 手動={fmt2(st['manual'])}")
            return {"ok": True}

        # 回=
        if text.startswith("回="):
            amount = parse_amount("回=")
            if amount is None:
                await tg_send_message(chat_id, "❌ 格式：回=100")
                return {"ok": True}

            st["ret"] = round(float(st["ret"]) + float(amount), 2)
            add_log(chat_id, chat_name, user, "回數", amount)
            await tg_send_message(chat_id, f"✅ 回數={fmt2(st['ret'])}")
            return {"ok": True}

        # 總計
        if text == "總計":
            balance = round(float(st["front"]) + float(st["manual"]) - float(st["ret"]), 2)
            await tg_send_message(
                chat_id,
                "📊 本群統計\n"
                f"前數：{fmt2(st['front'])}\n"
                f"手動：{fmt2(st['manual'])}\n"
                f"回數：{fmt2(st['ret'])}\n"
                "—\n"
                f"💰 餘額：{fmt2(balance)}",
            )
            return {"ok": True}

        # 清空
        if text == "清空":
            st["front"] = st["manual"] = st["ret"] = 0.0
            add_log(chat_id, chat_name, user, "清空", 0.0)
            save_db()
            await tg_send_message(chat_id, "🧹 已清空（前數/手動/回數）")
            return {"ok": True}

        # 狀態
        if text == "狀態":
            balance = round(float(st["front"]) + float(st["manual"]) - float(st["ret"]), 2)
            await tg_send_message(
                chat_id,
                "📌 目前狀態\n"
                f"前數：{fmt2(st['front'])}\n"
                f"手動：{fmt2(st['manual'])}\n"
                f"回數：{fmt2(st['ret'])}\n"
                f"餘額：{fmt2(balance)}\n"
                f"DATA_FILE：{DATA_FILE}",
            )
            return {"ok": True}

        # 查清空（最多5筆）
        if text == "查清空":
            logs = st.get("logs", [])
            clears = [x for x in logs if str(x.get("kind")) == "清空"][-5:]
            if not clears:
                await tg_send_message(chat_id, "✅ 目前沒有清空紀錄")
                return {"ok": True}

            lines = ["🧾 最近清空紀錄（最多5筆）"]
            for x in clears:
                lines.append(f"{x.get('time')} / {x.get('user')} / {x.get('chat_name')}")
            await tg_send_message(chat_id, "\n".join(lines))
            return {"ok": True}

        # 匯出（只回一行網址，key 留空讓你自己輸入）
        if text == "匯出":
            if not PUBLIC_BASE_URL:
                await tg_send_message(chat_id, "⚠️ 尚未設定 PUBLIC_BASE_URL")
                return {"ok": True}

            url = f"{PUBLIC_BASE_URL}/admin?chat_id={chat_id}&key="
            await tg_send_message(chat_id, url)
            return {"ok": True}

        return {"ok": True}

    except HTTPException:
        raise
    except Exception as e:
        print("WEBHOOK ERROR:", repr(e))
        print(traceback.format_exc())
        record_last_error(e)
        return {"ok": True}


@app.get("/admin", response_class=HTMLResponse)
async def admin_home(key: str, chat_id: Optional[int] = None):
    require_admin(key)

    chats = DB.get("chats", {})
    options = []
    for cid, obj in chats.items():
        name = obj.get("logs", [{}])[-1].get("chat_name", cid) if obj.get("logs") else cid
        sel = "selected" if (chat_id is not None and str(chat_id) == cid) else ""
        options.append(f"<option value='{cid}' {sel}>{cid} - {name}</option>")

    html = f"""
    <html>
    <head>
      <meta charset="utf-8">
      <title>Telegram 記帳後台</title>
    </head>
    <body style="font-family:Arial; padding:20px;">
      <h2>Telegram 記帳後台</h2>
      <p>資料檔：<code>{DATA_FILE}</code></p>

      <p>
        <a href="/setup-webhook?key={key}">✅ 設定 Webhook（首次部署點一次）</a>
      </p>

      <p>
        <a href="/debug_last_error?key={key}">🛠 查看最近 webhook 錯誤</a>
      </p>

      <h3>下載 Excel（含餘額）</h3>

      <div style="margin:10px 0;">
        <button type="button" onclick="setRange('today')">今天</button>
        <button type="button" onclick="setRange('yesterday')">昨天</button>
        <button type="button" onclick="setRange('last7')">過去7天</button>
        <button type="button" onclick="setRange('last30')">過去30天</button>
        <button type="button" onclick="setRange('thisMonth')">本月</button>
        <button type="button" onclick="setRange('lastMonth')">上月</button>
      </div>

      <form method="get" action="/admin/export">
        <input type="hidden" name="key" value="{key}">

        <div>
          <label>Chat：</label>
          <select name="chat_id" id="chat_id" required>
            {''.join(options)}
          </select>
        </div>

        <div style="margin-top:10px;">
          <label>開始：</label>
          <input type="datetime-local" step="1" id="start_dt" name="start" required>
          <small>（可到秒）</small>
        </div>

        <div style="margin-top:10px;">
          <label>結束：</label>
          <input type="datetime-local" step="1" id="end_dt" name="end" required>
          <small>（可到秒）</small>
        </div>

        <div style="margin-top:15px;">
          <button type="submit">下載</button>
        </div>
      </form>

      <script>
      function pad(n) {{ return String(n).padStart(2,'0'); }}
      function toLocalInputValue(d) {{
        return d.getFullYear() + '-' + pad(d.getMonth()+1) + '-' + pad(d.getDate()) +
               'T' + pad(d.getHours()) + ':' + pad(d.getMinutes()) + ':' + pad(d.getSeconds());
      }}

      function setRange(kind) {{
        const now = new Date();
        let start, end;

        if (kind === 'today') {{
          start = new Date(now.getFullYear(), now.getMonth(), now.getDate(), 0,0,0);
          end   = new Date(now.getFullYear(), now.getMonth(), now.getDate(), 23,59,59);
        }} else if (kind === 'yesterday') {{
          const y = new Date(now);
          y.setDate(y.getDate()-1);
          start = new Date(y.getFullYear(), y.getMonth(), y.getDate(), 0,0,0);
          end   = new Date(y.getFullYear(), y.getMonth(), y.getDate(), 23,59,59);
        }} else if (kind === 'last7') {{
          end = new Date(now);
          start = new Date(now);
          start.setDate(start.getDate()-7);
        }} else if (kind === 'last30') {{
          end = new Date(now);
          start = new Date(now);
          start.setDate(start.getDate()-30);
        }} else if (kind === 'thisMonth') {{
          start = new Date(now.getFullYear(), now.getMonth(), 1, 0,0,0);
          end = new Date(now.getFullYear(), now.getMonth(), now.getDate(), 23,59,59);
        }} else if (kind === 'lastMonth') {{
          const firstThis = new Date(now.getFullYear(), now.getMonth(), 1, 0,0,0);
          const lastPrev = new Date(firstThis.getTime() - 1000);
          start = new Date(lastPrev.getFullYear(), lastPrev.getMonth(), 1, 0,0,0);
          end = new Date(lastPrev.getFullYear(), lastPrev.getMonth(), lastPrev.getDate(), 23,59,59);
        }}

        document.getElementById('start_dt').value = toLocalInputValue(start);
        document.getElementById('end_dt').value = toLocalInputValue(end);
      }}

      // 預設過去30天
      setRange('last30');
      </script>

    </body>
    </html>
    """
    return HTMLResponse(html)


@app.get("/admin/export")
async def admin_export(key: str, chat_id: int, start: str, end: str):
    require_admin(key)
    try:
        start_dt = parse_dt_to_utc8(start)
        end_dt = parse_dt_to_utc8(end)
    except ValueError:
        raise HTTPException(status_code=400, detail="Bad datetime format")

    if end_dt < start_dt:
        raise HTTPException(status_code=400, detail="End must be >= Start")

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
    payload: Dict[str, Any] = {"url": webhook_url}
    if WEBHOOK_SECRET_TOKEN:
        payload["secret_token"] = WEBHOOK_SECRET_TOKEN

    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.post(f"{TG_API}/setWebhook", json=payload)
        data = r.json()
        if not data.get("ok"):
            raise HTTPException(status_code=500, detail=str(data))

    return RedirectResponse(url=f"/admin?key={key}")
