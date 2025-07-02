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

import aiofiles
import pandas as pd
import requests
from pytz import timezone
from playwright.async_api import async_playwright, Page, BrowserContext, TimeoutError

# ─── Environment-based secrets ─────────────────────────────────────────────────
LOGIN_USERNAME      = os.environ["OSP_USERNAME"]
LOGIN_PASSWORD      = os.environ["OSP_PASSWORD"]
GOOGLE_CHAT_WEBHOOK = os.environ["GCHAT_WEBHOOK"]
# ────────────────────────────────────────────────────────────────────────────────

# Constants
STORAGE_STATE           = 'state.json'
PRODUCT_DATA_FILE       = 'fRange.csv'
OSP_LOGIN_URL           = (
    'https://login.sso.osp.tech/oauth2/login?client_id=cpcollect&response_type=code&'
    'state=1728363996718-066611fb-c1d1-4abf-855b-39f72fa26a6d&'
    'scope=+openid+profile+retailer_id+sites+operation_id&'
    'redirect_uri=https%3A%2F%2Fcollect.morrisons.osp.tech%2Fverify.pandasso'
)
OSP_ORDERS_URL_TEMPLATE = 'https://collect.morrisons.osp.tech/orders?tab=other&date={date}'
FORM_URL                = 'https://docs.google.com/forms/d/e/1FAIpQLSfmctMksHosh0RQiUPdD4khE7DV273bzDvdt0BUN5b6JOQ_Wg/viewform'

# Globals
playwright_instance: Optional[async_playwright] = None
browser_instance:    Optional[BrowserContext]   = None
product_lookup_df:   Optional[pd.DataFrame]    = None

# ─── Logging setup ──────────────────────────────────────────────────────────────
def setup_logging():
    logger = logging.getLogger('extractor')
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
    logger.addHandler(handler)
    return logger

logger = setup_logging()

# ─── Helpers ───────────────────────────────────────────────────────────────────
async def ensure_storage_state():
    if not os.path.exists(STORAGE_STATE) or os.path.getsize(STORAGE_STATE) == 0:
        try:
            async with aiofiles.open(STORAGE_STATE, 'w') as f:
                await f.write('{}')
            logger.info("Initialized storage state.")
        except Exception as e:
            logger.error(f"Could not initialize storage state: {e}")

def create_google_chat_card(title: str, subtitle: str, items: list, link_text: str, link_url: str) -> Dict:
    if items and items[0].strip().startswith("<pre>"):
        text_content = items[0]
    else:
        safe_items = [str(i) for i in items if i is not None]
        text_content = "<br>".join(safe_items)

    return {
        "cardsV2": [{
            "cardId": "osp-report-" + str(datetime.datetime.now().timestamp()),
            "card": {
                "header": {"title": title, "subtitle": subtitle},
                "sections": [{
                    "widgets": [
                        {"textParagraph": {"text": text_content}},
                        {"buttonList": {"buttons": [
                            {"text": link_text, "onClick": {"openLink": {"url": link_url}}},
                            {"text": "Backup Report", "onClick": {"openLink": {"url": "https://lookerstudio.google.com/u/0/reporting/1gboaCxPhYIueczJu-2lqGpUUi6LXO5-d/page/DDJ9"}}}
                        ]}}
                    ]
                }]
            }
        }]
    }

def send_google_chat_message(title: str, subtitle: str, summary_items: list, link_text: str, link_url: str):
    if not GOOGLE_CHAT_WEBHOOK:
        logger.warning("Google Chat webhook not configured; skipping.")
        return
    headers = {"Content-Type": "application/json; charset=UTF-8"}
    payload = create_google_chat_card(title, subtitle, summary_items, link_text, link_url)
    try:
        resp = requests.post(GOOGLE_CHAT_WEBHOOK, headers=headers, json=payload, timeout=10)
        if resp.status_code not in (200, 204):
            logger.error(f"Google Chat send failed: {resp.status_code} {resp.text}")
        else:
            logger.info(f"Sent Google Chat message: {title}")
    except Exception as e:
        logger.error(f"Error sending Google Chat message: {e}")

# ── Playwright / OSP login & context ───────────────────────────────────────────
async def perform_login(page: Page) -> bool:
    try:
        logger.info(f"Navigating to login page")
        await page.goto(OSP_LOGIN_URL, timeout=30000)
        await page.wait_for_selector('input[placeholder="Username"]', timeout=20000)
        await page.fill('input[placeholder="Username"]', LOGIN_USERNAME)
        await page.fill('input[placeholder="Password"]', LOGIN_PASSWORD)
        await page.click('button:has-text("Log in")')
        await page.wait_for_url("https://collect.morrisons.osp.tech/orders*", timeout=20000)
        await page.wait_for_selector('div:has-text("Orders")', timeout=10000)
        logger.info("Login successful.")
        return True
    except TimeoutError as te:
        logger.error(f"Login timeout: {te}")
        return False
    except Exception as e:
        logger.error(f"Login error: {e}", exc_info=True)
        return False

async def login_and_get_context() -> Optional[BrowserContext]:
    global browser_instance
    if not browser_instance:
        logger.error("Browser not initialized.")
        return None

    context = None
    page = None
    try:
        state = STORAGE_STATE if os.path.exists(STORAGE_STATE) and os.path.getsize(STORAGE_STATE) > 2 else None
        context = await browser_instance.new_context(storage_state=state)
        page = await context.new_page()
        test_url = OSP_ORDERS_URL_TEMPLATE.format(date=datetime.date.today().strftime('%Y-%m-%d'))
        await page.goto(test_url, timeout=20000)

        if "login.sso.osp.tech" in page.url:
            logger.info("Session expired; doing full login.")
            await page.close()
            await context.close()
            context = await browser_instance.new_context()
            page = await context.new_page()
            if await perform_login(page):
                await context.storage_state(path=STORAGE_STATE)
                await page.close()
                return context
            else:
                await page.close()
                await context.close()
                return None
        else:
            await page.close()
            return context

    except Exception as e:
        logger.error(f"Error in login_and_get_context: {e}", exc_info=True)
        if page and not page.is_closed(): await page.close()
        if context and not getattr(context, '_closed', True): await context.close()
        return None

# ── Data extraction ─────────────────────────────────────────────────────────────
async def extract_osp_data(context: BrowserContext) -> Optional[Dict[str, Dict[str, str]]]:
    page = None
    orders_data: Dict[str, Dict[str, str]] = {}
    seen = set()
    tz = timezone('Europe/London')
    today = datetime.datetime.now(tz).date()
    target_dates = [today + datetime.timedelta(days=i) for i in range(1,4)]

    try:
        page = await context.new_page()
        for td in target_dates:
            page_num = 1
            date_str = td.strftime('%Y-%m-%d')
            url = OSP_ORDERS_URL_TEMPLATE.format(date=date_str)

            while True:
                if page_num == 1:
                    logger.info(f"Loading orders for {date_str}")
                    await page.goto(url, timeout=30000, wait_until='domcontentloaded')
                else:
                    logger.info(f"Paginating {date_str}, page {page_num}")

                try:
                    await page.wait_for_selector('tbody tr', timeout=15000)
                except TimeoutError:
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
                    nxt = await page.query_selector('button:has-text("Next"), button[aria-label="Next page"]')
                    if not (nxt and await nxt.is_enabled()):
                        break

                for ref in new_refs:
                    if ref in seen:
                        continue
                    logger.info(f"Processing order {ref}")
                    row = await page.query_selector(f'tbody tr:has-text("{re.escape(ref)}")')
                    if not row:
                        seen.add(ref)
                        continue

                    try:
                        await row.click(timeout=10000)
                        await page.wait_for_selector('div:has-text("Order contents"), h2:has-text("Order contents")', timeout=15000)
                    except Exception as e:
                        logger.error(f"Error opening {ref}: {e}")
                        seen.add(ref)
                        await page.goto(url, timeout=20000, wait_until='domcontentloaded')
                        continue

                    await page.screenshot(path=f'output/order_{ref}.png')
                    full_text = ""
                    for sel in ['main','div[role="main"]','article','section#main-content','div.page-content','div.order-detail-container','body']:
                        try:
                            loc = page.locator(sel)
                            if await loc.count() > 0:
                                txt = await loc.first.inner_text(timeout=7000)
                                if ("Order contents" in txt and "Product ID" in txt) or ("Order reference:" in txt and "Collection slot:" in txt):
                                    full_text = txt
                                    break
                        except Exception:
                            pass

                    slot = date_str
                    m = re.search(r'Collection slot:\s*(\d{2}-\d{2}-\d{4})', full_text)
                    if m:
                        try:
                            slot = datetime.datetime.strptime(m.group(1), "%d-%m-%Y").strftime("%Y-%m-%d")
                        except Exception:
                            pass

                    orders_data[ref] = {'details': full_text, 'collection_slot': slot}
                    seen.add(ref)

                    # Go back
                    try:
                        back = page.locator('a:has-text("BACK"), button:has-text("BACK")')
                        if await back.count() > 0:
                            await back.first.click(timeout=10000)
                            await page.wait_for_selector('tbody tr', timeout=15000)
                        else:
                            raise Exception("No BACK button")
                    except Exception:
                        await page.goto(url, timeout=20000, wait_until='domcontentloaded')

                nxt = await page.query_selector('button:has-text("Next"), button[aria-label="Next page"]')
                if nxt and await nxt.is_enabled():
                    await nxt.click(timeout=5000)
                    await page.wait_for_load_state('networkidle', timeout=15000)
                    page_num += 1
                else:
                    break

        # Save CSV
        if orders_data:
            os.makedirs('output', exist_ok=True)
            df = pd.DataFrame.from_dict(orders_data, orient='index')
            df.index.name = 'Order Reference'
            df.to_csv(os.path.join('output','extracted_orders_data.csv'))
            # Optional Google Form
            if FORM_URL:
                for ref, info in orders_data.items():
                    await fill_google_form({"Field 1": ref, "Field 2": info['details'], "Field 3": info['collection_slot']})
        return orders_data

    except Exception as e:
        logger.error(f"extract_osp_data error: {e}", exc_info=True)
        return None

    finally:
        if page and not page.is_closed():
            await page.close()

# ── Google Form filler ─────────────────────────────────────────────────────────
async def fill_google_form(order_data: Dict[str,str]):
    if not FORM_URL:
        return
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            ctx = await browser.new_context()
            pg  = await ctx.new_page()
            await pg.goto(FORM_URL, timeout=60000, wait_until='domcontentloaded')

            xpaths = {
                "Field 1": "//*[@id='mG61Hd']/div[2]/div/div[2]/div[1]/div/div/div[2]/div/div[1]/div[2]/textarea",
                "Field 2": "//*[@id='mG61Hd']/div[2]/div/div[2]/div[2]/div/div/div[2]/div/div[1]/div[2]/textarea",
                "Field 3": "//*[@id='mG61Hd']/div[2]/div/div[2]/div[3]/div/div/div[2]/div/div[1]/div[2]/textarea",
                "Submit": "//*[@id='mG61Hd']/div[2]/div/div[3]/div[1]/div[1]/div/span/span"
            }
            await pg.fill(xpaths["Field 1"], str(order_data["Field 1"]))
            await pg.fill(xpaths["Field 2"], str(order_data["Field 2"]))
            await pg.fill(xpaths["Field 3"], str(order_data["Field 3"]))
            await pg.click(xpaths["Submit"])
            await pg.wait_for_selector("text=/Your response has been recorded|Another response/i", timeout=30000)

            await pg.close()
            await ctx.close()
            await browser.close()
    except Exception as e:
        logger.error(f"fill_google_form error for {order_data.get('Field 1')}: {e}", exc_info=True)

# ── Summary generator ─────────────────────────────────────────────────────────
async def generate_daily_item_summary(orders_data: Optional[Dict[str,Dict[str,str]]], prod_df: Optional[pd.DataFrame]) -> Optional[str]:
    if prod_df is None or prod_df.empty:
        return "Product lookup data not available."
    tz = timezone('Europe/London')
    next_day = datetime.datetime.now(tz) + datetime.timedelta(days=1)
    key = next_day.strftime('%Y-%m-%d')
    disp = next_day.strftime('%d/%m/%y')

    items_by_dept = defaultdict(lambda: defaultdict(int))
    lookup = {min_val:(row['Item Name'], row['Department']) for min_val, row in prod_df.iterrows()}

    total = 0
    texts = []
    for ref, info in (orders_data or {}).items():
        if info.get('collection_slot') == key:
            total += 1
            texts.append(info['details'])

    matches = 0
    for txt in texts:
        found = False
        for min_val,(name,dept) in lookup.items():
            cnt = len(re.findall(r'\b'+re.escape(min_val)+r'\b', txt))
            if cnt:
                items_by_dept[dept][min_val] += cnt
                found = True
        if found:
            matches += 1

    lines = [f"Orders for {disp}:", f"{total} orders total, {matches} with matching products.", ""]
    if total == 0:
        lines.append("No orders scheduled for this day.")
    elif not items_by_dept:
        lines.append("No items from the product list were found.")
    else:
        for dept,mins in sorted(items_by_dept.items()):
            lines.append(f"{dept}:")
            for min_val,cnt in sorted(mins.items()):
                name,_ = lookup.get(min_val,("Unknown",""))
                lines.append(f"  {min_val:<9}  {name} *{cnt}")
            lines.append("")

    return "\n".join(lines).strip()

# ─── Main extraction orchestration ─────────────────────────────────────────────
async def perform_osp_extraction() -> bool:
    timestamp = datetime.datetime.now(timezone('Europe/London')).strftime('%d/%m/%y %H:%M')
    await ensure_storage_state()

    ctx = await login_and_get_context()
    if not ctx:
        send_google_chat_message(
            "OSP Extraction - Login Failure",
            f"Attempted: {timestamp}",
            ["Login failed; could not obtain session."],
            "Backup Report",
            "https://lookerstudio.google.com/u/0/reporting/1gboaCxPhYIueczJu-2lqGpUUi6LXO5-d/page/DDJ9"
        )
        return False

    orders = await extract_osp_data(ctx)
    await ctx.close()

    if orders is None:
        send_google_chat_message(
            "OSP Extraction - Data Failure",
            f"Attempted: {timestamp}",
            ["Data extraction failed at a critical step."],
            "Backup Report",
            "https://lookerstudio.google.com/u/0/reporting/1gboaCxPhYIueczJu-2lqGpUUi6LXO5-d/page/DDJ9"
        )
        return False

    # Daily counts
    base = datetime.datetime.now(timezone('Europe/London')).date()
    summary_counts = []
    any_orders = False
    for i in range(1,4):
        d = base + datetime.timedelta(days=i)
        key = d.strftime('%Y-%m-%d')
        cnt = sum(1 for v in orders.values() if v.get('collection_slot') == key)
        if cnt:
            any_orders = True
        summary_counts.append(f"{cnt} orders for {d.strftime('%d/%m/%y')}")
    if not any_orders:
        summary_counts = ["No orders found for the next 3 days."]

    send_google_chat_message(
        "OSP Extraction - Daily Order Counts",
        f"Report Time: {timestamp}",
        summary_counts,
        "View Dashboard",
        "https://lookerstudio.google.com/embed/reporting/65cb4d97-37d3-4de9-aab2-096b5d753b96/page/p_3uhgsgcvld"
    )

    # Item summary
    item_summary = await generate_daily_item_summary(orders, product_lookup_df)
    next_disp = (base + datetime.timedelta(days=1)).strftime('%d/%m/%y')
    if item_summary:
        send_google_chat_message(
            f"OSP Items - For Collection {next_disp}",
            f"Generated: {timestamp}",
            [f"<pre>{item_summary}</pre>"],
            "View Dashboard",
            "https://lookerstudio.google.com/embed/reporting/65cb4d97-37d3-4de9-aab2-096b5d753b96/page/p_3uhgsgcvld"
        )
    return True

# ─── Entrypoint ────────────────────────────────────────────────────────────────
async def main() -> bool:
    global playwright_instance, browser_instance, product_lookup_df

    # Start Playwright
    playwright_instance = await async_playwright().start()
    browser_instance    = await playwright_instance.chromium.launch(
        headless=True,
        args=['--no-sandbox','--disable-setuid-sandbox','--disable-dev-shm-usage','--disable-gpu']
    )

    # Load product lookup
    try:
        df = pd.read_csv(PRODUCT_DATA_FILE, header=1, skip_blank_lines=True)
        df.columns = [str(c).strip() for c in df.columns]
        df.dropna(subset=['MIN'], inplace=True)
        df['MIN'] = df['MIN'].astype(float).astype(int).astype(str)
        df['Item Name'] = df['Item Name'].astype(str).fillna('Unknown Item')
        df['Department'] = df['Department'].astype(str).fillna('Unknown Department')
        df.set_index('MIN', inplace=True)
        product_lookup_df = df
        logger.info(f"Loaded {PRODUCT_DATA_FILE} ({len(df)} items).")
    except Exception as e:
        logger.error(f"Failed to load product data: {e}", exc_info=True)
        product_lookup_df = pd.DataFrame(columns=['Item Name','Department']).set_index(pd.Index([], name='MIN'))

    # Run extraction
    success = await perform_osp_extraction()

    # Clean up
    try:
        await browser_instance.close()
        await playwright_instance.stop()
    except Exception as e:
        logger.error(f"Error during cleanup: {e}", exc_info=True)

    return success

if __name__ == "__main__":
    result = asyncio.run(main())
    sys.exit(0 if result else 1)
