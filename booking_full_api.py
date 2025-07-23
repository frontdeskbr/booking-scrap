#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
booking_full_api.py

API FastAPI que:
 • faz scraping (imagens, descrição, facilidades, preços do calendário)
 • grava os dados em Supabase (tabela booking_ads), usando url_hash como chave.
"""

import os, re, time, hashlib, datetime as dt, logging
from typing import List, Dict
from urllib.parse import urlsplit, urlunsplit

import requests
from bs4 import BeautifulSoup
from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

# Selenium
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Supabase
from supabase import create_client, Client
from dotenv import load_dotenv
load_dotenv()

# ─── Logging ─────────────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO, datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ─── Supabase ─────────────────────────────────────────────────────────────
SUPA_URL = os.getenv("SUPABASE_URL")
SUPA_KEY = os.getenv("SUPABASE_KEY")
if not SUPA_URL or not SUPA_KEY:
    raise RuntimeError("Defina SUPABASE_URL e SUPABASE_KEY no .env")
supabase: Client = create_client(SUPA_URL, SUPA_KEY)

# ─── Configurações ────────────────────────────────────────────────────────
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/115.0.0.0 Safari/537.36"
    )
}
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", 30))
MAX_CAL_MONTHS   = int(os.getenv("MAX_CAL_MONTHS", 12))

app = FastAPI(
    title="Booking Full Scraper API",
    version="1.2.0",
    description="Scrapes Booking.com listings and stores results in Supabase",
)

# ─── Habilita CORS para todas as origens ───────────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],            # ou especifique uma lista de domínios
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─── Utilitários ──────────────────────────────────────────────────────────
def canonicalize_url(raw_url: str) -> str:
    parts = urlsplit(raw_url)
    clean = urlunsplit((parts.scheme, parts.netloc, parts.path, '', ''))
    return clean.rstrip('/')

def url_md5(text: str) -> str:
    return hashlib.md5(text.encode('utf-8')).hexdigest()

def _get_soup(url: str) -> BeautifulSoup:
    try:
        resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        return BeautifulSoup(resp.text, "html.parser")
    except Exception as e:
        logger.error(f"Falha no GET: {e}")
        raise HTTPException(status_code=502, detail=f"Erro ao buscar página: {e}")

# ─── Scrapers ─────────────────────────────────────────────────────────────
def scrape_images_and_details(url: str):
    soup = _get_soup(url)
    image_urls: List[str] = []
    for img in soup.find_all("img", src=True):
        src = img["src"]
        if "/xdata/images/hotel/" in src:
            full = requests.compat.urljoin(url, src)
            if full not in image_urls:
                image_urls.append(full)
    desc_tag = soup.find("p", {"data-testid": "property-description"})
    description = desc_tag.get_text(strip=True) if desc_tag else ""
    fac_tags = soup.select('div[data-testid="property-most-popular-facilities-wrapper"] li')
    main_facilities = [li.get_text(strip=True) for li in fac_tags]
    logger.info(f"Imagens: {len(image_urls)} | Facilidades: {len(main_facilities)}")
    return image_urls, description, main_facilities

def scrape_calendar_prices(url: str) -> Dict[str, int]:
    opts = Options()
    opts.binary_location = "/usr/bin/chromium"
    opts.add_argument("--headless")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")

    driver = None
    prices: Dict[str, int] = {}
    try:
        service = Service("/usr/bin/chromedriver")
        driver = webdriver.Chrome(service=service, options=opts)
        wait = WebDriverWait(driver, 15)
        driver.get(url)
        wait.until(EC.element_to_be_clickable(
            (By.CSS_SELECTOR, "[data-testid='searchbox-dates-container'] button")
        )).click()

        for _ in range(MAX_CAL_MONTHS):
            cal = wait.until(EC.presence_of_element_located(
                (By.CSS_SELECTOR, "[data-testid='searchbox-datepicker-calendar']")
            ))
            for cell in cal.find_elements(By.CSS_SELECTOR, "span[data-date]"):
                date = cell.get_attribute("data-date")
                try:
                    price_text = cell.find_element(
                        By.CSS_SELECTOR, "div.a91bd87e91 span.e7362e5f34"
                    ).text
                    price = int(re.sub(r"[^\d]", "", price_text)) if price_text else None
                    if price:
                        prices[date] = price
                except:
                    continue
            try:
                cal.find_element(
                    By.CSS_SELECTOR,
                    "button[aria-label*='seguinte'], button[aria-label*='next']"
                ).click()
                time.sleep(0.35)
            except:
                break
    except Exception as e:
        logger.error(f"Erro Selenium: {e}")
    finally:
        if driver:
            driver.quit()
    logger.info(f"Preços capturados: {len(prices)} datas")
    return dict(sorted(prices.items()))

# ─── Persistência ─────────────────────────────────────────────────────────
def save_to_supabase(data: dict) -> dict:
    resp = (
        supabase.table("booking_ads")
                .upsert(data, on_conflict="url_hash", ignore_duplicates=False)
                .execute()
    )
    if getattr(resp, "data", None):
        logger.info("Registro salvo/atualizado.")
        return resp.data[0]
    raise HTTPException(status_code=500, detail="Resposta vazia do Supabase")

# ─── Endpoints ────────────────────────────────────────────────────────────
@app.get("/scrape", response_class=JSONResponse)
def scrape(url: str = Query(..., description="URL completa do anúncio no Booking.com")):
    logger.info(f"Scraping iniciado: {url}")
    canonical = canonicalize_url(url)
    hsh       = url_md5(canonical)
    imgs, desc, facs = scrape_images_and_details(canonical)
    cal_prices       = scrape_calendar_prices(canonical)

    payload = {
        "url": canonical,
        "url_hash": hsh,
        "image_urls": imgs,
        "description": desc,
        "main_facilities": facs,
        "calendar_prices": cal_prices,
        "scraped_at": dt.datetime.utcnow().isoformat(),
    }
    row = save_to_supabase(payload)
    return {"status":"success", "data": row}

@app.get("/health")
def health_check():
    return {"status":"healthy","timestamp":dt.datetime.utcnow().isoformat()}

@app.get("/ads")
def list_ads(limit: int = Query(10, description="Máximo de anúncios retornados")):
    r = (supabase.table("booking_ads")
               .select("*")
               .order("scraped_at", desc=True)
               .limit(limit)
               .execute())
    return {"status":"success","count": len(r.data or []),"ads": r.data or []}

@app.get("/ads/{ad_id}")
def get_ad(ad_id: int):
    r = supabase.table("booking_ads").select("*").eq("id", ad_id).execute()
    if not r.data:
        raise HTTPException(status_code=404, detail="Anúncio não encontrado")
    return {"status":"success","data": r.data[0]}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("booking_full_api:app", host="0.0.0.0", port=int(os.getenv("PORT",8000)), reload=True)
