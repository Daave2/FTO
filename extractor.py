#!/usr/bin/env python3
import asyncio
import os
import sys
import logging
import json
import re
import datetime
from typing import Optional, Dict
from collections import defaultdict
from pathlib import Path

import aiofiles
import pandas as pd
import requests
from pytz import timezone
from playwright.async_api import async_playwright, Page, BrowserContext, TimeoutError

# ─── Setup output directory ─────────────────────────────────────────────────────
OUTPUT_DIR = Path('output')
OUTPUT_DIR.mkdir(exist_ok=True)
STORAGE_STATE = 'state.json'
PRODUCT_DATA_FILE = 'fRange.csv'

# ─── Environment-based secrets ─────────────────────────────────────────────────
LOGIN_USERNAME      = os.environ.get("OSP_USERNAME", "")
LOGIN_PASSWORD      = os.environ.get("OSP_PASSWORD", "")
GOOGLE_CHAT_WEBHOOK = os.environ.get("GCHAT_WEBHOOK", "")
# ────────────────────────────────────────────────────────────────────────────────

# ─── URLs ────────────────────────────────────────────────────────────────────────
OSP_LOGIN_URL = (
    'https://login.sso.osp.tech/oauth2/login?client_id=cpcollect&response_type=code&'
    'state=1728363996718-066611fb-c1d1-4abf-855b-39f72fa26a6d&'
    'scope=+openid+profile+retailer_id+sites+operation_id&'
    'redirect_uri=https%3A%2F%2Fcollect.morrisons.osp.tech%2Fverify.pandasso'
)
OSP_ORDERS_URL_TEMPLATE = 'https://collect.morrisons.osp.tech/orders?tab=other&date={date}'
FORM_URL = 'https://docs.google.com/forms/d/e/1FAIpQLSfmctMksHosh0RQiUPdD4khE7DV273bzDvdt0BUN5b6JOQ_Wg/viewform'
# ────────────────────────────────────────────────────────────────────────────────

# ─── Globals ────────────────────────────────────────────────────────────────────
playwright_instance: Optional[async_playwright] = None
browser_instance:    Optional[BrowserContext]   = None
product_lookup_df:   Optional[pd.DataFrame]    = None
# ────────────────────────────────────────────────────────────────────────────────

# ─── Logging setup ──────────────────────────────────────────────────────────────
def setup_logging():
    logger = logging.getLogger('extractor')
    logger.setLevel(logging.INFO)

    fmt = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    fh = logging.FileHandler(OUTPUT_DIR / 'extractor.log')
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    return logger

logger = setup_logging()
# ────────────────────────────────────────────────────────────────────────────────

def timestamp() -> str:
    return datetime.datetime.now().strftime('%Y%m%d_%H%M%S')

async def ensure_storage_state():
    if not os.path.exists(STORAGE_STATE) or os.path.getsize(STORAGE_STATE) == 0:
        try:
            async with aiofiles.open(STORAGE_STATE, 'w') as f:
                await f.write('{}')
            logger.info("Initialized storage state.")
        except Exception as e:
            logger.error(f"Could not initialize storage state: {e}")

async def dump_page_state(page: Page, name: str):
    """
    Take a full-page screenshot and HTML dump synchronously,
    before any context/page is closed.
    """
    ts = timestamp()
    png = OUTPUT_DIR / f"{name}_{ts}.png"
    html = OUTPUT_DIR / f"{name}_{ts}.html"
    try:
        await page.screenshot(path=str(png), full_page=True)
        logger.info(f"🖼 Screenshot saved: {png}")
    except Exception as e:
        logger.error(f"❌ Screenshot failed ({png}): {e}")
    try:
        content = await page.content()
        async with aiofiles.open(html, 'w') as f:
            await f.write(content)
        logger.info(f"📄 HTML dump saved: {html}")
    except Exception as e:
        logger.error(f"❌ HTML dump failed ({html}): {e}")

def create_google_chat_card(title: str, subtitle: str, items: list, link_text: str, link_url: str) -> Dict:
    text = items[0] if items and items[0].strip().startswith("<pre>") else "<br>".join(str(i) for i in items)
    return {
        "cardsV2": [{
            "cardId": f"osp-report-{timestamp()}",
            "card": {
                "header": {"title": title, "subtitle": subtitle},
                "sections": [{
                    "widgets": [
                        {"textParagraph": {"text": text}},
                        {"buttonList": {"buttons": [
                            {"text": link_text,   "onClick": {"openLink": {"url": link_url}}},
                            {"text": "Backup Rep", "onClick": {"openLink": {"url": "https://lookerstudio.google.com/u/0/reporting/1gboaCxPhYIueczJu-2lqGpUUi6LXO5-d/page/DDJ9"}}}
                        ]}}
                    ]
                }]
            }
        }]
    }

def send_google_chat_message(title: str, subtitle: str, summary_items: list, link_text: str, link_url: str):
    if not GOOGLE_CHAT_WEBHOOK:
        logger.warning("Google Chat webhook not set; skipping.")
        return
    headers = {"Content-Type":"application/json; charset=UTF-8"}
    payload = create_google_chat_card(title, subtitle, summary_items, link_text, link_url)
    try:
        r = requests.post(GOOGLE_CHAT_WEBHOOK, headers=headers, json=payload, timeout=10)
        if r.status_code not in (200,204):
            logger.error(f"Chat failed: {r.status_code} {r.text}")
        else:
            logger.info(f"Sent chat: {title}")
    except Exception as e:
        logger.error(f"Chat error: {e}")

async def perform_login(page: Page) -> bool:
    try:
        logger.info("→ goto login page")
        await page.goto(OSP_LOGIN_URL, timeout=60000, wait_until="networkidle")
        logger.info("→ wait for Username field")
        await page.wait_for_selector('input[placeholder="Username"]', timeout=30000)
        await page.fill('input[placeholder="Username"]', LOGIN_USERNAME)
        await page.fill('input[placeholder="Password"]', LOGIN_PASSWORD)
        await page.click('button:has-text("Log in")')
        logger.info("→ waiting for orders URL")
        logger.debug(f"   current URL: {page.url}")
        await page.wait_for_url("https://collect.morrisons.osp.tech/orders*", timeout=60000, wait_until="networkidle")
        await page.wait_for_selector('div:has-text("Orders")', timeout=30000)
        logger.info("✅ Login successful.")
        return True

    except TimeoutError as te:
        logger.error(f"⏱ Login timeout: {te}")
        await dump_page_state(page, "login_timeout")
        return False

    except Exception as e:
        logger.error(f"❗Login error: {e}", exc_info=True)
        await dump_page_state(page, "login_error")
        return False

async def login_and_get_context() -> Optional[BrowserContext]:
    global browser_instance
    if not browser_instance:
        logger.critical("Browser not initialized.")
        return None

    # Try existing storage state
    state = STORAGE_STATE if os.path.exists(STORAGE_STATE) else None
    ctx = await browser_instance.new_context(storage_state=state)
    page = await ctx.new_page()
    test_url = OSP_ORDERS_URL_TEMPLATE.format(date=datetime.date.today().strftime('%Y-%m-%d'))
    await page.goto(test_url, timeout=20000)

    if "login.sso.osp.tech" in page.url:
        logger.info("🔄 Session expired; performing full login")
        await page.close()
        await ctx.close()

        ctx = await browser_instance.new_context()
        page = await ctx.new_page()
        ok = await perform_login(page)
        if ok:
            await ctx.storage_state(path=STORAGE_STATE)
            await page.close()
            return ctx
        else:
            logger.error("❌ perform_login() failed; aborting")
            await page.close()
            await ctx.close()
            return None
    else:
        logger.info("✔ Session valid; reusing context")
        await page.close()
        return ctx

async def extract_osp_data(context: BrowserContext) -> Optional[Dict[str,Dict[str,str]]]:
    page = None
    data = {}
    seen = set()
    tz = timezone('Europe/London')
    today = datetime.datetime.now(tz).date()
    dates = [today + datetime.timedelta(days=i) for i in range(1,4)]

    try:
        page = await context.new_page()
        for d in dates:
            date_str = d.strftime('%Y-%m-%d')
            url = OSP_ORDERS_URL_TEMPLATE.format(date=date_str)
            page_num = 1

            while True:
                if page_num == 1:
                    logger.info(f"→ loading {date_str}")
                    await page.goto(url, timeout=30000, wait_until='domcontentloaded')
                else:
                    logger.info(f"→ paginating {date_str} p{page_num}")

                try:
                    await page.wait_for_selector('tbody tr', timeout=15000)
                except TimeoutError:
                    logger.info("   no rows found; next date")
                    break

                rows = await page.query_selector_all('tbody tr')
                new_refs = []
                for i in range(len(rows)):
                    rows = await page.query_selector_all('tbody tr')
                    cell = await rows[i].query_selector('td:first-child')
                    if cell:
                        ref = (await cell.inner_text()).strip()
                        if ref and ref not in seen:
                            new_refs.append(ref)

                if not new_refs:
                    nxt = await page.query_selector('button:has-text("Next"),button[aria-label="Next page"]')
                    if not (nxt and await nxt.is_enabled()):
                        break

                for ref in new_refs:
                    if ref in seen:
                        continue
                    logger.info(f"   • processing {ref}")
                    try:
                        row = await page.query_selector(f'tbody tr:has-text("{re.escape(ref)}")')
                        await row.click(timeout=10000)
                        await page.wait_for_selector('div:has-text("Order contents"),h2:has-text("Order contents")',timeout=15000)
                    except Exception as e:
                        logger.error(f"open details {ref}: {e}")
                        await dump_page_state(page, f"open_error_{ref}")
                        seen.add(ref)
                        await page.goto(url, timeout=20000, wait_until='domcontentloaded')
                        continue

                    await dump_page_state(page, f"details_{ref}")

                    # extract text
                    text = ""
                    for sel in ['main','div[role="main"]','article','section#main-content','div.page-content','div.order-detail-container','body']:
                        try:
                            loc = page.locator(sel)
                            if await loc.count() > 0:
                                t = await loc.first.inner_text(timeout=7000)
                                if "Order contents" in t and "Collection slot:" in t:
                                    text = t
                                    break
                        except Exception:
                            pass

                    slot = date_str
                    m = re.search(r'Collection slot:\s*(\d{2}-\d{2}-\d{4})', text)
                    if m:
                        try:
                            slot = datetime.datetime.strptime(m.group(1), "%d-%m-%Y").strftime("%Y-%m-%d")
                        except Exception:
                            pass

                    data[ref] = {'details': text, 'collection_slot': slot}
                    seen.add(ref)

                    # go back
                    try:
                        back = page.locator('a:has-text("BACK"),button:has-text("BACK")')
                        if await back.count() > 0:
                            await back.first.click(timeout=10000)
                            await page.wait_for_selector('tbody tr',timeout=15000)
                        else:
                            raise Exception("no BACK")
                    except Exception:
                        await page.goto(url, timeout=20000, wait_until='domcontentloaded')

                nxt = await page.query_selector('button:has-text("Next"),button[aria-label="Next page"]')
                if nxt and await nxt.is_enabled():
                    await nxt.click(timeout=5000)
                    await page.wait_for_load_state('networkidle',timeout=15000)
                    page_num += 1
                else:
                    break

        if data:
            df = pd.DataFrame.from_dict(data,orient='index')
            df.index.name = 'Order Reference'
            df.to_csv(OUTPUT_DIR / 'extracted_orders_data.csv')
            logger.info(f"Saved CSV ({len(data)} orders).")
            if FORM_URL:
                for ref,info in data.items():
                    await fill_google_form({"Field 1":ref,"Field 2":info['details'],"Field 3":info['collection_slot']})
        return data

    except Exception as e:
        logger.error(f"extract error: {e}", exc_info=True)
        if page and not page.is_closed():
            await dump_page_state(page, "extract_error")
        return None

    finally:
        if page and not page.is_closed():
            await page.close()

async def fill_google_form(order_data: Dict[str,str]):
    if not FORM_URL:
        return
    try:
        async with async_playwright() as p:
            b = await p.chromium.launch(headless=True)
            ctx = await b.new_context()
            pg  = await ctx.new_page()
            await pg.goto(FORM_URL,timeout=60000,wait_until='domcontentloaded')

            x = {
                "Field 1":"//*[@id='mG61Hd']/div[2]/div/div[2]/div[1]//textarea",
                "Field 2":"//*[@id='mG61Hd']/div[2]/div/div[2]/div[2]//textarea",
                "Field 3":"//*[@id='mG61Hd']/div[2]/div/div[2]/div[3]//textarea",
                "Submit":"//*[@id='mG61Hd']//span[text()='Submit']"
            }
            await pg.fill(x["Field 1"],str(order_data["Field 1"]))
            await pg.fill(x["Field 2"],str(order_data["Field 2"]))
            await pg.fill(x["Field 3"],str(order_data["Field 3"]))
            await pg.click(x["Submit"])
            await pg.wait_for_selector("text=/Your response has been recorded|Another response/i",timeout=30000)
            await pg.close(); await ctx.close(); await b.close()
    except Exception as e:
        logger.error(f"form error for {order_data.get('Field 1')}: {e}",exc_info=True)

async def generate_daily_item_summary(orders, prod_df) -> Optional[str]:
    if prod_df is None or prod_df.empty:
        return "Product data unavailable."
    tz = timezone('Europe/London')
    nd = datetime.datetime.now(tz)+datetime.timedelta(days=1)
    key,disp = nd.strftime('%Y-%m-%d'), nd.strftime('%d/%m/%y')

    by_dept=defaultdict(lambda:defaultdict(int))
    lookup={min_val:(row['Item Name'],row['Department']) for min_val,row in prod_df.iterrows()}

    total=0; texts=[]
    for info in (orders or {}).values():
        if info.get('collection_slot')==key:
            total+=1; texts.append(info['details'])

    matches=0
    for t in texts:
        found=False
        for min_val,(name,dept) in lookup.items():
            cnt=len(re.findall(r'\b'+re.escape(min_val)+r'\b',t))
            if cnt:
                by_dept[dept][min_val]+=cnt; found=True
        if found: matches+=1

    lines=[f"Orders for {disp}:",f"{total} total, {matches} with matches.",""]
    if total==0:
        lines.append("No orders scheduled.")
    elif not by_dept:
        lines.append("No known items found.")
    else:
        for dept,mins in sorted(by_dept.items()):
            lines.append(f"{dept}:")
            for m,c in sorted(mins.items()):
                name,_=lookup.get(m,("Unknown",""))
                lines.append(f"  {m:<9} {name} *{c}")
            lines.append("")
    return "\n".join(lines).strip()

async def main() -> bool:
    global playwright_instance,browser_instance,product_lookup_df

    playwright_instance = await async_playwright().start()
    browser_instance    = await playwright_instance.chromium.launch(
        headless=True, args=['--no-sandbox','--disable-dev-shm-usage','--disable-gpu']
    )

    # load product file
    try:
        df = pd.read_csv(PRODUCT_DATA_FILE,header=1,skip_blank_lines=True)
        df.columns=[str(c).strip() for c in df.columns]
        df.dropna(subset=['MIN'],inplace=True)
        df['MIN']=df['MIN'].astype(float).astype(int).astype(str)
        df['Item Name']=df['Item Name'].astype(str).fillna('Unknown')
        df['Department']=df['Department'].astype(str).fillna('Unknown')
        df.set_index('MIN',inplace=True)
        product_lookup_df=df
        logger.info(f"Loaded {PRODUCT_DATA_FILE} ({len(df)} items).")
    except Exception as e:
        logger.error(f"Load product error: {e}",exc_info=True)
        product_lookup_df=pd.DataFrame(columns=['Item Name','Department']).set_index(pd.Index([],name='MIN'))

    await ensure_storage_state()
    ctx=await login_and_get_context()
    if not ctx:
        return False

    orders=await extract_osp_data(ctx)
    await ctx.close()

    if orders is None:
        return False

    tz = timezone('Europe/London')
    ts = datetime.datetime.now(tz).strftime('%d/%m/%y %H:%M')

    # daily counts
    base = datetime.datetime.now(tz).date()
    counts=[]; any_=False
    for i in range(1,4):
        d=base+datetime.timedelta(days=i)
        key=d.strftime('%Y-%m-%d')
        c=sum(1 for v in orders.values() if v.get('collection_slot')==key)
        if c: any_=True
        counts.append(f"{c} orders for {d.strftime('%d/%m/%y')}")
    if not any_:
        counts=["No orders next 3 days."]

    send_google_chat_message(
        "OSP Extraction – Daily Order Counts",
        f"Time: {ts}",
        counts,
        "Dashboard",
        "https://lookerstudio.google.com/embed/reporting/…"
    )

    summary = await generate_daily_item_summary(orders, product_lookup_df)
    if summary:
        send_google_chat_message(
            f"OSP Items for { (base+datetime.timedelta(days=1)).strftime('%d/%m/%y') }",
            f"Time: {ts}",
            [f"<pre>{summary}</pre>"],
            "Dashboard",
            "https://lookerstudio.google.com/embed/reporting/…"
        )

    # cleanup
    await browser_instance.close()
    await playwright_instance.stop()
    return True

if __name__ == "__main__":
    ok = asyncio.run(main())
    sys.exit(0 if ok else 1)
