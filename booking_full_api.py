#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
booking_full_api.py

API FastAPI que:
 • faz scraping (imagens, descrição, facilidades, preços do calendário)
 • grava os dados em Supabase (tabela booking_ads), usando url_hash como chave.
"""

import os, re, time, json, hashlib, datetime as dt, logging
from typing import List, Dict
from urllib.parse import urlsplit, urlunsplit

import requests
from bs4 import BeautifulSoup
from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import JSONResponse

# Selenium
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

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
SUPA_KEY = os.getenv("SUPABASE_KEY")      # prefira a Service Role key

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
MAX_CAL_MONTHS = int(os.getenv("MAX_CAL_MONTHS", 12))

app = FastAPI(
    title="Booking Full Scraper API",
    version="1.2.0",
    description="Scrapes Booking.com listings and stores results in Supabase",
)

# ─── Utilitários ──────────────────────────────────────────────────────────
def canonicalize_url(raw_url: str) -> str:
    """
    Remove parâmetros (“?…”) e fragmentos (“#…”) da URL
    para evitar excesso de tamanho e padronizar o valor salvo.
    """
    parts = urlsplit(raw_url)
    clean = urlunsplit((parts.scheme, parts.netloc, parts.path, '', ''))
    return clean.rstrip('/')

def url_md5(text: str) -> str:
    return hashlib.md5(text.encode('utf-8')).hexdigest()

def _get_soup(url: str) -> BeautifulSoup:
    try:
        resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        # usa o parser nativo do Python em vez do lxml
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
    opts.add_argument("--headless")
    opts.add_argument("--window-size=1200,800")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")

    driver = None
    try:
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
        wait = WebDriverWait(driver, 15)
        driver.get(url)

        wait.until(EC.element_to_be_clickable(
            (By.CSS_SELECTOR, "[data-testid='searchbox-dates-container'] button")
        )).click()

        prices: Dict[str, int] = {}
        for m in range(MAX_CAL_MONTHS):
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
                except Exception:
                    continue

            try:
                cal.find_element(
                    By.CSS_SELECTOR,
                    "button[aria-label='Mês seguinte'], button[aria-label*='seguinte']"
                ).click()
                time.sleep(0.35)
            except Exception:
                break

        logger.info(f"Preços capturados: {len(prices)} datas")
        return dict(sorted(prices.items()))
    except Exception as e:
        logger.error(f"Erro Selenium: {e}")
        return {}
    finally:
        if driver:
            driver.quit()

# ─── Persistência ─────────────────────────────────────────────────────────
def save_to_supabase(data: dict) -> dict:
    try:
        resp = (
            supabase.table("booking_ads")
            .upsert(data, on_conflict="url_hash", ignore_duplicates=False)
            .execute()
        )
        if getattr(resp, "data", None):
            logger.info("Registro salvo/atualizado.")
            return resp.data[0]
        raise ValueError("Resposta vazia do Supabase")
    except Exception as e:
        logger.error(f"Erro BD: {e}")
        raise HTTPException(status_code=500, detail=f"Erro ao salvar no banco: {e}")

# ─── End-points ───────────────────────────────────────────────────────────
@app.get("/scrape", response_class=JSONResponse)
def scrape(url: str = Query(..., description="URL completa do anúncio no Booking.com")):
    logger.info(f"Scraping iniciado: {url}")

    canonical_url = canonicalize_url(url)
    url_hash      = url_md5(canonical_url)

    imgs, desc, facs = scrape_images_and_details(url)
    cal_prices        = scrape_calendar_prices(url)

    payload = {
        "url": canonical_url,
        "url_hash": url_hash,
        "image_urls": imgs,
        "description": desc,
        "main_facilities": facs,
        "calendar_prices": cal_prices,
        "scraped_at": dt.datetime.utcnow().isoformat(),
    }

    row = save_to_supabase(payload)

    return {
        "status": "success",
        "message": "Dados extraídos e gravados no Supabase",
        "data": {
            "id": row.get("id"),
            "url": row["url"],
            "image_count": len(imgs),
            "facilities_count": len(facs),
            "price_dates_count": len(cal_prices),
            "scraped_at": row.get("scraped_at"),
        },
    }

@app.get("/health")
def health_check():
    return {"status": "healthy", "timestamp": dt.datetime.utcnow().isoformat(), "version": "1.2.0"}

@app.get("/ads")
def list_ads(limit: int = Query(10, description="Máximo de anúncios retornados")):
    try:
        r = supabase.table("booking_ads") \
            .select("id, url, scraped_at, updated_at") \
            .order("scraped_at", desc=True).limit(limit).execute()
        return {"status": "success", "count": len(r.data or []), "ads": r.data or []}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao listar: {e}")

@app.get("/ads/{ad_id}")
def get_ad(ad_id: int):
    try:
        r = supabase.table("booking_ads").select("*").eq("id", ad_id).execute()
        if not r.data:
            raise HTTPException(status_code=404, detail="Anúncio não encontrado")
        return {"status": "success", "data": r.data[0]}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro BD: {e}")

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("booking_full_api:app", host="0.0.0.0", port=port, reload=True)
