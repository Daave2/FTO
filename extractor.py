#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
OSP Orders Extractor – tables in Google Chat + Email (shared renderer)
- Faster row parsing and pagination
- Robust login/session reuse
- Optional Google Form + Chat + Email
- CLI flags, retries, graceful shutdown
- HTML tables rendered identically in Email and Google Chat
"""

import asyncio
import os
import sys
import logging
import json
import re
import datetime as dt
from typing import Optional, Dict, List, Any, Tuple
from collections import defaultdict
from pathlib import Path
import signal
import argparse

import smtplib
from email.message import EmailMessage

import aiofiles
import pandas as pd
import requests
from pytz import timezone
from playwright.async_api import (
    async_playwright,
    Page,
    Browser,
    BrowserContext,
    TimeoutError as PWTimeout,
    Error as PWError,
)

# ─── Paths ─────────────────────────────────────────────────────────────────────
OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(exist_ok=True)
STORAGE_STATE = "state.json"
PRODUCT_DATA_FILE = "fRange.csv"
LOG_FILE = OUTPUT_DIR / "extractor.log"

# ─── URLs ──────────────────────────────────────────────────────────────────────
OSP_LOGIN_URL = (
    "https://login.sso.osp.tech/oauth2/login?client_id=cpcollect&response_type=code&"
    "state=1728363996718-066611fb-c1d1-4abf-855b-39f72fa26a6d&"
    "scope=+openid+profile+retailer_id+sites+operation_id&"
    "redirect_uri=https%3A%2F%2Fcollect.morrisons.osp.tech%2Fverify.pandasso"
)
OSP_ORDERS_URL_TEMPLATE = "https://collect.morrisons.osp.tech/orders?tab=other&date={date}"
FORM_URL = "https://docs.google.com/forms/d/e/1FAIpQLSfmctMksHosh0RQiUPdD4khE7DV273bzDvdt0BUN5b6JOQ_Wg/viewform"
DASHBOARD_URL = "https://lookerstudio.google.com/embed/reporting/65cb4d97-37d3-4de9-aab2-096b5d753b96/page/p_3uhgsgcvld"

# ─── Env ───────────────────────────────────────────────────────────────────────
LOGIN_USERNAME = os.environ.get("OSP_USERNAME", "")
LOGIN_PASSWORD = os.environ.get("OSP_PASSWORD", "")
GOOGLE_CHAT_WEBHOOK = os.environ.get("GCHAT_WEBHOOK", "")
SMTP_SERVER = os.environ.get("SMTP_SERVER", "")
SMTP_PORT = os.environ.get("SMTP_PORT", "465")
SMTP_USER = os.environ.get("SMTP_USER", "")
SMTP_PASS = os.environ.get("SMTP_PASS", "")
EMAIL_TO = os.environ.get("EMAIL_TO", "")

# ─── Globals ───────────────────────────────────────────────────────────────────
TZ = timezone("Europe/London")
play: Optional[async_playwright] = None
browser: Optional[Browser] = None

# ─── Logging ───────────────────────────────────────────────────────────────────
def setup_logging() -> logging.Logger:
    logger = logging.getLogger("extractor")
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    fh = logging.FileHandler(LOG_FILE)
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    return logger

logger = setup_logging()

# ─── Small utils ───────────────────────────────────────────────────────────────
def now_str(fmt="%Y%m%d_%H%M%S") -> str:
    return dt.datetime.now(TZ).strftime(fmt)

async def ensure_storage_state():
    if not os.path.exists(STORAGE_STATE) or os.path.getsize(STORAGE_STATE) == 0:
        try:
            async with aiofiles.open(STORAGE_STATE, "w") as f:
                await f.write("{}")
            logger.info("Initialized storage state.")
        except Exception as e:
            logger.error(f"Could not initialize storage state: {e}")

async def dump_page_state(page: Page, name: str):
    ts = now_str()
    png = OUTPUT_DIR / f"{name}_{ts}.png"
    html = OUTPUT_DIR / f"{name}_{ts}.html"
    try:
        await page.screenshot(path=str(png), full_page=True)
        logger.info(f"🖼 Screenshot saved: {png}")
    except Exception as e:
        logger.error(f"❌ Screenshot failed ({png}): {e}")
    try:
        content = await page.content()
        async with aiofiles.open(html, "w") as f:
            await f.write(content)
        logger.info(f"📄 HTML dump saved: {html}")
    except Exception as e:
        logger.error(f"❌ HTML dump failed ({html}): {e}")

async def a_retry(coro_fn, *args, retries=3, base_sleep=0.8, on_error=None, **kwargs):
    for i in range(retries):
        try:
            return await coro_fn(*args, **kwargs)
        except (PWTimeout, PWError) as e:
            if on_error:
                on_error(e, i)
            if i == retries - 1:
                raise
            await asyncio.sleep(base_sleep * (2**i))

async def wait_for_any(page: Page, selectors: List[str], timeout=15000) -> Optional[str]:
    tasks = [page.wait_for_selector(sel, timeout=timeout) for sel in selectors]
    try:
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for p in pending:
            p.cancel()
        if done:
            for sel in selectors:
                if await page.locator(sel).count() > 0:
                    return sel
    except Exception:
        pass
    return None

# ─── Shared HTML table builder (Email + Chat) ──────────────────────────────────
def build_summary_tables_html(summary_data: Dict[str, Any], prod_df: pd.DataFrame) -> str:
    """Return HTML with one table per department. Used by both Email and Google Chat."""
    if not summary_data or not summary_data.get("by_dept") or prod_df is None or prod_df.empty:
        return "<i>No matched items found.</i>"

    html = []
    # style attributes kept simple for chat/email
    table_open = (
        "<table border='1' cellspacing='0' cellpadding='4' "
        "style='border-collapse:collapse; width:100%; font-family:sans-serif; font-size:12px;'>"
        "<tr><th>MIN</th><th>Item</th><th>Count</th></tr>"
    )

    for dept, mins in summary_data["by_dept"].items():
        html.append(f"<h4 style='margin:8px 0'>{dept}</h4>")
        html.append(table_open)
        for m, cnt in mins.items():
            name = prod_df.loc[m, "Item Name"] if m in prod_df.index else "Unknown"
            if isinstance(name, pd.Series):
                name = name.iloc[0]
            html.append(f"<tr><td>{m}</td><td>{name}</td><td>{cnt}</td></tr>")
        html.append("</table><br>")
    return "".join(html)

# ─── Google Chat Cards ─────────────────────────────────────────────────────────
def _chat_buttons(link_url: str) -> List[Dict[str, Any]]:
    return [
        {"text": "Main Report", "onClick": {"openLink": {"url": link_url}}},
        {
            "text": "Backup Rep",
            "onClick": {
                "openLink": {
                    "url": "https://lookerstudio.google.com/u/0/reporting/1gboaCxPhYIueczJu-2lqGpUUi6LXO5-d/page/DDJ9"
                }
            },
        },
    ]

def create_daily_counts_card(title: str, subtitle: str, counts: List[str], link_url: str) -> Dict[str, Any]:
    widgets = [{"decoratedText": {"startIcon": {"knownIcon": "TICKET"}, "text": item}} for item in counts]
    return {
        "cardsV2": [
            {
                "cardId": f"osp-daily-counts-{now_str()}",
                "card": {
                    "header": {
                        "title": title,
                        "subtitle": subtitle,
                        "imageUrl": "https://img.icons8.com/office/80/calendar-today.png",
                        "imageType": "CIRCLE",
                    },
                    "sections": [
                        {"header": "Upcoming Order Totals", "widgets": widgets, "collapsible": True, "uncollapsibleWidgetsCount": 3},
                        {"widgets": [{"buttonList": {"buttons": _chat_buttons(link_url)}}]},
                    ],
                },
            }
        ]
    }

def create_item_summary_card(title: str, subtitle: str, summary_data: Dict[str, Any], link_url: str, prod_df: pd.DataFrame) -> Dict[str, Any]:
    """Google Chat card with the same HTML tables as the email."""
    stats_grid = {
        "grid": {
            "title": "Summary Stats",
            "columnCount": 2,
            "borderStyle": {"type": "STROKE"},
            "items": [
                {"title": str(summary_data.get("total_orders", "N/A")), "subtitle": "Total Orders"},
                {"title": str(summary_data.get("matched_orders", "N/A")), "subtitle": "Orders with Items"},
            ],
        }
    }
    tables_html = build_summary_tables_html(summary_data, prod_df)
    summary_paragraph = {"textParagraph": {"text": tables_html}}

    return {
        "cardsV2": [
            {
                "cardId": f"osp-item-summary-{now_str()}",
                "card": {
                    "header": {
                        "title": title,
                        "subtitle": subtitle,
                        "imageUrl": "https://img.icons8.com/color/96/shopping-cart--v1.png",
                        "imageType": "CIRCLE",
                    },
                    "sections": [
                        {"widgets": [stats_grid]},
                        {"header": "Item Details", "widgets": [summary_paragraph]},
                        {"widgets": [{"buttonList": {"buttons": _chat_buttons(link_url)}}]},
                    ],
                },
            }
        ]
    }

def send_google_chat_message(payload: Dict[str, Any], title_for_log: str):
    if not GOOGLE_CHAT_WEBHOOK:
        logger.info("Google Chat webhook not set; skipping chat send.")
        return
    try:
        with requests.Session() as s:
            r = s.post(GOOGLE_CHAT_WEBHOOK, headers={"Content-Type": "application/json; charset=UTF-8"}, json=payload, timeout=15)
            r.raise_for_status()
        logger.info(f"Sent chat: {title_for_log}")
    except requests.exceptions.HTTPError as he:
        logger.error(f"Chat failed '{title_for_log}': {he.response.status_code} {he.response.text}")
    except Exception as e:
        logger.error(f"Chat error '{title_for_log}': {e}")

# ─── Email ─────────────────────────────────────────────────────────────────────
def send_orders_email(file_path: Path, summary_data: Optional[Dict[str, Any]] = None, prod_df: Optional[pd.DataFrame] = None):
    if not SMTP_SERVER or not EMAIL_TO:
        logger.info("SMTP settings not configured; skipping email.")
        return

    msg = EmailMessage()
    msg["Subject"] = f"OSP Orders Extract {now_str()}"
    msg["From"] = SMTP_USER or EMAIL_TO
    msg["To"] = EMAIL_TO

    plain = "Attached are the extracted FTO lines for tomorrow's collection."
    html_body = "<p>Attached are the extracted FTO lines for tomorrow's collection.</p>"

    # Use the same HTML builder as Chat
    if summary_data and prod_df is not None and not prod_df.empty:
        html_body += "<h3>Item Summary</h3>"
        html_body += build_summary_tables_html(summary_data, prod_df)

    msg.set_content(plain)
    msg.add_alternative(f"<html><body>{html_body}</body></html>", subtype="html")

    try:
        with open(file_path, "rb") as f:
            msg.add_attachment(f.read(), maintype="application", subtype="octet-stream", filename=file_path.name)
    except Exception as e:
        logger.error(f"Failed to attach {file_path}: {e}")
        return

    try:
        port = int(SMTP_PORT) if str(SMTP_PORT).isdigit() else 0
        if port == 465:
            server = smtplib.SMTP_SSL(SMTP_SERVER, port)
        else:
            server = smtplib.SMTP(SMTP_SERVER, port)
            server.starttls()
        if SMTP_USER and SMTP_PASS:
            server.login(SMTP_USER, SMTP_PASS)
        server.send_message(msg)
        server.quit()
        logger.info("Orders email sent.")
    except Exception as e:
        logger.error(f"Email send failed: {e}")

# ─── Login + Context ───────────────────────────────────────────────────────────
async def perform_login(page: Page) -> bool:
    try:
        logger.info("→ goto login page")
        await page.goto(OSP_LOGIN_URL, timeout=60000, wait_until="networkidle")
        await page.wait_for_selector('input[placeholder="Username"]', timeout=30000)
        await page.fill('input[placeholder="Username"]', LOGIN_USERNAME)
        await page.fill('input[placeholder="Password"]', LOGIN_PASSWORD)
        await page.click('button:has-text("Log in")')
        await page.wait_for_url("https://collect.morrisons.osp.tech/orders*", timeout=60000)
        await page.wait_for_selector('text=Orders', timeout=30000)
        logger.info("✅ Login successful.")
        return True
    except PWTimeout as te:
        logger.error(f"⏱ Login timeout: {te}")
        await dump_page_state(page, "login_timeout")
    except Exception as e:
        logger.error(f"❗Login error: {e}", exc_info=True)
        await dump_page_state(page, "login_error")
    return False

async def login_and_get_context(b: Browser) -> Optional[BrowserContext]:
    state = STORAGE_STATE if os.path.exists(STORAGE_STATE) else None
    ctx = await b.new_context(storage_state=state)
    page = await ctx.new_page()
    try:
        test_url = OSP_ORDERS_URL_TEMPLATE.format(date=dt.date.today().strftime("%Y-%m-%d"))
        await page.goto(test_url, timeout=25000)
        if "login.sso.osp.tech" in page.url:
            logger.info("🔄 Session expired; performing full login")
            await page.close()
            await ctx.close()
            ctx = await b.new_context()
            page = await ctx.new_page()
            if await perform_login(page):
                await ctx.storage_state(path=STORAGE_STATE)
                return ctx
            await ctx.close()
            return None
        else:
            logger.info("✔ Session valid; reusing context")
            return ctx
    finally:
        try:
            if not page.is_closed():
                await page.close()
        except Exception:
            pass

# ─── Extraction ────────────────────────────────────────────────────────────────
async def _collect_refs_from_table(page: Page) -> List[str]:
    refs: List[str] = []
    rows = await page.query_selector_all("tbody tr")
    for row in rows:
        cell = await row.query_selector("td:first-child")
        if not cell:
            continue
        ref = (await cell.text_content() or "").strip()
        if ref:
            refs.append(ref)
    return refs

async def _open_order(page: Page, ref: str) -> bool:
    row = await page.query_selector(f'tbody tr:has(td:first-child:has-text("{ref}"))')
    if not row:
        row = await page.query_selector(f'tbody tr:has-text("{ref}")')
    if not row:
        return False
    await row.click(timeout=10000)
    sel = await wait_for_any(page, ['text="Order contents"', 'h2:has-text("Order contents")'], timeout=20000)
    return bool(sel)

async def _extract_order_text(page: Page) -> Tuple[str, str]:
    best = ""
    for sel in ["main", 'div[role="main"]', "article", "section#main-content", "div.page-content", "body"]:
        try:
            loc = page.locator(sel)
            if await loc.count() > 0:
                t = await loc.first.text_content(timeout=6000)
                if t and "Order contents" in t:
                    best = t
                    break
        except Exception:
            pass
    slot = dt.date.today().strftime("%Y-%m-%d")
    m = re.search(r"Collection slot:\s*(\d{2}-\d{2}-\d{4})", best or "")
    if m:
        try:
            slot = dt.datetime.strptime(m.group(1), "%d-%m-%Y").strftime("%Y-%m-%d")
        except Exception:
            pass
    return best or "", slot

async def extract_osp_data(context: BrowserContext, days: int) -> Optional[Dict[str, Dict[str, str]]]:
    page = await context.new_page()
    data: Dict[str, Dict[str, str]] = {}
    seen: set[str] = set()

    try:
        today = dt.datetime.now(TZ).date()
        dates = [today + dt.timedelta(days=i) for i in range(1, max(1, days) + 1)]

        for d in dates:
            date_str = d.strftime("%Y-%m-%d")
            url = OSP_ORDERS_URL_TEMPLATE.format(date=date_str)
            page_num = 1

            while True:
                if page_num == 1:
                    logger.info(f"→ loading {date_str}")
                    await page.goto(url, timeout=45000, wait_until="domcontentloaded")
                else:
                    logger.info(f"→ paginating {date_str} p{page_num}")

                try:
                    await page.wait_for_selector("tbody tr", timeout=15000)
                except PWTimeout:
                    logger.info("   no rows found; next date")
                    break

                refs = await _collect_refs_from_table(page)
                new_refs = [r for r in refs if r not in seen]

                if not new_refs:
                    nxt = await page.query_selector('button:has-text("Next"),button[aria-label="Next page"]')
                    if nxt and await nxt.is_enabled():
                        await nxt.click(timeout=7000)
                        await page.wait_for_load_state("networkidle", timeout=15000)
                        page_num += 1
                        continue
                    break

                for ref in new_refs:
                    seen.add(ref)
                    logger.info(f"   • processing {ref}")
                    try:
                        ok = await a_retry(_open_order, page, ref, retries=2, base_sleep=0.8)
                        if not ok:
                            logger.warning(f"      could not open details for {ref}")
                            await dump_page_state(page, f"open_error_{ref}")
                            await page.goto(url, timeout=20000, wait_until="domcontentloaded")
                            continue

                        details, slot = await _extract_order_text(page)
                        data[ref] = {"details": details, "collection_slot": slot}

                        back = await page.query_selector('a:has-text("BACK"),button:has-text("BACK")')
                        if back and await back.is_enabled():
                            await back.click(timeout=8000)
                            await page.wait_for_selector("tbody tr", timeout=15000)
                        else:
                            await page.goto(url, timeout=20000, wait_until="domcontentloaded")

                    except Exception as e:
                        logger.error(f"open/extract {ref}: {e}")
                        await dump_page_state(page, f"extract_error_{ref}")
                        try:
                            await page.goto(url, timeout=20000, wait_until="domcontentloaded")
                        except Exception:
                            pass

                nxt = await page.query_selector('button:has-text("Next"),button[aria-label="Next page"]')
                if nxt and await nxt.is_enabled():
                    await nxt.click(timeout=7000)
                    await page.wait_for_load_state("networkidle", timeout=15000)
                    page_num += 1
                else:
                    break

        if data:
            df = pd.DataFrame.from_dict(data, orient="index")
            df.index.name = "Order Reference"
            out = OUTPUT_DIR / "extracted_orders_data.csv"
            df.to_csv(out)
            logger.info(f"Saved CSV ({len(data)} orders) -> {out}")
        return data

    except Exception as e:
        logger.error(f"extract error: {e}", exc_info=True)
        await dump_page_state(page, "extract_error")
        return None
    finally:
        try:
            if not page.is_closed():
                await page.close()
        except Exception:
            pass

# ─── Google Form (reusing context) ─────────────────────────────────────────────
async def fill_google_form(context: BrowserContext, order_data: Dict[str, str]):
    if not FORM_URL:
        return
    pg = await context.new_page()
    try:
        await pg.goto(FORM_URL, timeout=60000, wait_until="domcontentloaded")
        x = {
            "Field 1": "//*[@id='mG61Hd']/div[2]/div/div[2]/div[1]//textarea|//*[@id='mG61Hd']/div[2]/div/div[2]/div[1]//input",
            "Field 2": "//*[@id='mG61Hd']/div[2]/div/div[2]/div[2]//textarea|//*[@id='mG61Hd']/div[2]/div/div[2]/div[2]//input",
            "Field 3": "//*[@id='mG61Hd']/div[2]/div/div[2]/div[3]//textarea|//*[@id='mG61Hd']/div[2]/div/div[2]/div[3]//input",
            "Submit": "//*[@id='mG61Hd']//span[text()='Submit']",
        }
        await pg.fill(x["Field 1"], str(order_data.get("Field 1", "")))
        await pg.fill(x["Field 2"], str(order_data.get("Field 2", "")))
        await pg.fill(x["Field 3"], str(order_data.get("Field 3", "")))
        await pg.click(x["Submit"])
        await pg.wait_for_selector("text=/Your response has been recorded|Another response/i", timeout=30000)
    except Exception as e:
        logger.error(f"form error for {order_data.get('Field 1')}: {e}", exc_info=True)
        await dump_page_state(pg, "form_error")
    finally:
        try:
            await pg.close()
        except Exception:
            pass

# ─── Summary generation ────────────────────────────────────────────────────────
def load_product_lookup(path: str) -> pd.DataFrame:
    try:
        df = pd.read_csv(path, header=1, skip_blank_lines=True)
        df.columns = [str(c).strip() for c in df.columns]
        df.dropna(subset=["MIN"], inplace=True)
        df["MIN"] = df["MIN"].astype(float).astype(int).astype(str)
        df["Item Name"] = df["Item Name"].astype(str).fillna("Unknown")
        df["Department"] = df["Department"].astype(str).fillna("Unknown")
        df.drop_duplicates(subset="MIN", keep="first", inplace=True)
        df.set_index("MIN", inplace=True)
        logger.info(f"Loaded {path} ({len(df)} items).")
        return df
    except Exception as e:
        logger.error(f"Load product error: {e}", exc_info=True)
        return pd.DataFrame(columns=["Item Name", "Department"]).set_index(pd.Index([], name="MIN"))

def compile_min_regex(mins: List[str]) -> Dict[str, re.Pattern]:
    return {m: re.compile(rf"\b{re.escape(m)}\b") for m in mins}

async def generate_daily_item_summary(orders: Dict[str, Dict[str, str]], prod_df: pd.DataFrame) -> Dict[str, Any]:
    if prod_df is None or prod_df.empty:
        return {"summary_text": "Product data unavailable.", "total_orders": 0, "matched_orders": 0, "by_dept": {}}

    nd = dt.datetime.now(TZ) + dt.timedelta(days=1)
    key_date = nd.strftime("%Y-%m-%d")
    disp = nd.strftime("%d/%m/%y")

    by_dept: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
    lookup = {min_val: (row["Item Name"], row["Department"]) for min_val, row in prod_df.iterrows()}

    texts = [info["details"] for info in orders.values() if info.get("collection_slot") == key_date]
    total_orders = len([1 for _ in orders.values() if _.get("collection_slot") == key_date])

    if not texts:
        return {
            "summary_text": f"Orders for {disp}:\n0 total, 0 with item matches.\n\nNo orders scheduled.",
            "total_orders": 0,
            "matched_orders": 0,
            "by_dept": {},
        }

    patterns = compile_min_regex(list(lookup.keys()))
    matched_orders = 0

    for t in texts:
        found = False
        for min_val, pat in patterns.items():
            matches = pat.findall(t)
            if matches:
                name, dept = lookup[min_val]
                by_dept[dept][min_val] += len(matches)
                found = True
        if found:
            matched_orders += 1

    lines = [f"Orders for {disp}:", f"{total_orders} total, {matched_orders} with item matches.", ""]
    if not by_dept:
        lines.append("No known items found in tomorrow's orders.")
    else:
        for dept, mins in sorted(by_dept.items()):
            lines.append(f"<b>{dept}:</b>")
            for m, c in sorted(mins.items()):
                name, _ = lookup.get(m, ("Unknown", ""))
                lines.append(f"  {m:<9} {name} *<b>{c}</b>")
            lines.append("")

    return {
        "summary_text": "\n".join(lines).strip(),
        "total_orders": total_orders,
        "matched_orders": matched_orders,
        "by_dept": {d: dict(v) for d, v in by_dept.items()},
    }

# ─── Main ──────────────────────────────────────────────────────────────────────
def validate_env() -> None:
    missing = []
    if not LOGIN_USERNAME: missing.append("OSP_USERNAME")
    if not LOGIN_PASSWORD: missing.append("OSP_PASSWORD")
    if missing:
        logger.warning(f"Missing env: {', '.join(missing)}")

async def run(days: int, headful: bool, skip_form: bool, skip_chat: bool, skip_email: bool) -> bool:
    global play, browser
    validate_env()
    await ensure_storage_state()

    play = await async_playwright().start()
    browser = await play.chromium.launch(
        headless=not headful,
        args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
    )

    product_df = load_product_lookup(PRODUCT_DATA_FILE)

    ctx = await login_and_get_context(browser)
    if not ctx:
        await browser.close()
        await play.stop()
        return False

    orders = await extract_osp_data(ctx, days=days)
    if orders is None:
        await ctx.close()
        await browser.close()
        await play.stop()
        return False

    if not skip_form and FORM_URL:
        for ref, info in orders.items():
            try:
                await fill_google_form(ctx, {"Field 1": ref, "Field 2": info["details"], "Field 3": info["collection_slot"]})
            except Exception as e:
                logger.error(f"Form fill failed for {ref}: {e}")

    ts = dt.datetime.now(TZ).strftime("%d/%m/%y %H:%M")
    base_date = dt.datetime.now(TZ).date()

    # Daily counts (next N days)
    counts = []
    any_orders = False
    for i in range(1, days + 1):
        d = base_date + dt.timedelta(days=i)
        key = d.strftime("%Y-%m-%d")
        count = sum(1 for v in orders.values() if v.get("collection_slot") == key)
        if count > 0:
            any_orders = True
        counts.append(f"<b>{count} orders</b> for {d.strftime('%a, %d %b')}")
    if not any_orders:
        counts = ["No orders found for the chosen period."]

    if not skip_chat:
        counts_payload = create_daily_counts_card("OSP Extraction – Daily Order Counts", f"Ran at: {ts} (UK)", counts, DASHBOARD_URL)
        send_google_chat_message(counts_payload, "Daily Order Counts")

    # Tomorrow summary (now includes HTML tables in Chat)
    summary_data = await generate_daily_item_summary(orders, product_df)
    if not skip_chat:
        tomorrow_str = (base_date + dt.timedelta(days=1)).strftime("%d/%m/%y")
        summary_payload = create_item_summary_card(
            f"OSP Item Summary for {tomorrow_str}",
            f"Ran at: {ts} (UK)",
            summary_data,
            DASHBOARD_URL,
            product_df,
        )
        send_google_chat_message(summary_payload, f"Item Summary for {tomorrow_str}")

    if not skip_email:
        send_orders_email(OUTPUT_DIR / "extracted_orders_data.csv", summary_data=summary_data, prod_df=product_df)

    await ctx.close()
    await browser.close()
    await play.stop()
    return True

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Extract OSP orders and post summaries.")
    p.add_argument("--days", type=int, default=3, help="How many days ahead to scan (default 3).")
    p.add_argument("--headful", action="store_true", help="Run browser headful for debugging.")
    p.add_argument("--skip-form", action="store_true", help="Skip Google Form submission.")
    p.add_argument("--skip-chat", action="store_true", help="Skip Google Chat messages.")
    p.add_argument("--skip-email", action="store_true", help="Skip sending email.")
    return p.parse_args()

def main():
    args = parse_args()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    stop = asyncio.Event()

    def _handle_sig(*_):
        stop.set()

    for s in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(s, _handle_sig)
        except NotImplementedError:
            pass

    try:
        ok = loop.run_until_complete(run(args.days, args.headful, args.skip_form, args.skip_chat, args.skip_email))
        sys.exit(0 if ok else 1)
    finally:
        loop.close()

if __name__ == "__main__":
    main()