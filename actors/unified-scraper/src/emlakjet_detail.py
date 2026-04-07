"""
emlakjet_detail.py — Emlakjet.com tekil ilan URL scraper.

requests + JSON-LD Product + BeautifulSoup CSS seciciler — Selenium yok, hizli.
Emlakjet artik Next.js App Router kullaniyor (__next_s), __NEXT_DATA__ yok.
Veri kaynaklari (oncelik sirasi):
  1. JSON-LD <script type="application/ld+json"> Product
  2. og:description meta tag'i (fiyat, m2, konum)
  3. BeautifulSoup CSS seciciler (quickInfoList, currentPrice, vs)
"""
from __future__ import annotations

import json
import re
from typing import Optional

import requests
from bs4 import BeautifulSoup

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "tr-TR,tr;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    "Referer": "https://www.emlakjet.com/",
}


def _fetch_html(url: str, timeout: int = 15) -> str:
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=timeout, allow_redirects=True)
        if resp.status_code in (403, 429, 503):
            return ""
        return resp.text
    except Exception as exc:
        print(f"[EMLAKJET] Fetch hatasi ({url[:70]}): {exc}")
        return ""


def _parse_price(text: str) -> Optional[int]:
    """'13.000.000 TL' veya '13000000' gibi metinden tam sayi dondurur."""
    digits = re.sub(r"[^\d]", "", text)
    return int(digits) if digits else None


def _parse_m2(text: str) -> Optional[int]:
    """'95 m²' gibi metinden m2 deger dondurur."""
    m = re.search(r"(\d+)\s*m", text, re.I)
    return int(m.group(1)) if m else None


def scrape_url(url: str) -> Optional[dict]:
    """Emlakjet.com ilan detay URL'sini scrape et. dict veya None doner."""
    html = _fetch_html(url)
    if not html or len(html) < 500:
        return None

    soup = BeautifulSoup(html, "lxml")

    # --- 1. JSON-LD Product ---
    product: dict = {}
    for sc in soup.find_all("script", type="application/ld+json"):
        try:
            d = json.loads(sc.string or "")
            if d.get("@type") == "Product":
                product = d
                break
        except Exception:
            pass

    # title
    title = (
        product.get("name")
        or (soup.find("h1") or BeautifulSoup("", "lxml")).get_text(strip=True)
    )
    if not title:
        return None

    # price — JSON-LD offers.price en guvenilir kaynak
    price: Optional[int] = None
    currency = "TRY"
    offers = product.get("offers") or {}
    if offers.get("price"):
        try:
            price = int(float(str(offers["price"]).replace(",", ".")))
        except Exception:
            pass
    currency = str(offers.get("priceCurrency") or "TRY")

    if price is None:
        # Fallback: currentPrice element
        price_el = soup.find(class_=re.compile(r"currentPrice", re.I))
        if price_el:
            price = _parse_price(price_el.get_text(strip=True))

    # --- 2. og:description parsing (ilce, sehir, m2) ---
    og_desc = ""
    og_title = ""
    for m in soup.find_all("meta"):
        prop = m.get("property", "")
        if "og:description" in prop:
            og_desc = m.get("content", "")
        elif "og:title" in prop:
            og_title = m.get("content", "")

    # og:description ornegi: "Yelken Emlak İstanbul Kadıköy Caddebostan Mahallesi 95 m² 2+1 Oda 13,000,000 TL ..."
    city = ""
    district = ""
    neighborhood = ""
    net_m2: Optional[int] = None
    rooms = ""

    # Sehir/ilce/mahalle: buyuk harf ile baslayan art arda kelimeler, "Mahallesi" sonrasi
    city_m = re.search(r"\b(İstanbul|Ankara|İzmir|Bursa|Antalya|Adana|Konya|Kayseri|Gaziantep|Mersin)\b", og_desc or og_title)
    if city_m:
        city = city_m.group(1)

    dist_m = re.search(city + r"\s+([A-ZÇĞİÖŞÜa-zçğışöüA-Z][a-zçğışöüA-Z]+(?:\s+[A-ZÇĞİÖŞÜa-zçğışöüA-Z][a-zçğışöüA-Z]+)?)\s+Mahallesi", og_desc or og_title)
    if dist_m:
        district = dist_m.group(1)
    elif city:
        after_city = re.search(city + r"\s+(\S+)", og_desc or og_title)
        if after_city:
            district = after_city.group(1)

    mah_m = re.search(r"(\S+(?:\s+\S+)?)\s+Mahallesi", og_desc or og_title)
    if mah_m:
        neighborhood = mah_m.group(1)

    # m2
    m2_m = re.search(r"(\d+)\s*m²", og_desc or og_title)
    if m2_m:
        net_m2 = int(m2_m.group(1))

    # rooms: "2+1 Oda"
    room_m = re.search(r"(\d+\+\d+)\s*Oda", og_desc or og_title)
    if room_m:
        rooms = room_m.group(1)

    # --- 3. BeautifulSoup detay alanlari ---
    # quickInfoList (oda, kat, m2 ikon satirlari)
    info_wrap = soup.find(class_=re.compile(r"quickInfoList|infoList", re.I))
    info_items: list[str] = []
    if info_wrap:
        info_items = [
            i.get_text(strip=True)
            for i in info_wrap.find_all(["li", "span", "div"])
            if i.get_text(strip=True)
        ]

    if not rooms:
        for item in info_items:
            if re.match(r"\d+\+\d+", item):
                rooms = item
                break

    if net_m2 is None:
        for item in info_items:
            m2_val = _parse_m2(item)
            if m2_val and m2_val > 10:
                net_m2 = m2_val
                break

    # floor: "X. Kat" veya "Yuksek giris" gibi degerler
    floor = ""
    for item in info_items:
        if re.search(r"kat|giris|cati", item, re.I):
            floor = item
            break

    # images
    images = []
    img_src_set = set()
    if product.get("image"):
        images.append(product["image"])
        img_src_set.add(product["image"])
    for img in soup.find_all("img", src=re.compile(r"imaj\.emlakjet", re.I)):
        src = img.get("src", "")
        # Kucuk thumbnail'leri atla (resize/292 gibi)
        if src and "resize/292" not in src and src not in img_src_set:
            images.append(src)
            img_src_set.add(src)

    # description (infoDescription veya aciklama alani)
    desc_el = soup.find(class_=re.compile(r"infoDescription|description", re.I))
    description = desc_el.get_text(strip=True)[:500] if desc_el else ""

    # Asansor, otopark, kredi bilgisi (infoList metinden)
    all_text = " ".join(info_items).lower()
    # Ayrica og:description'dan
    full_text = (all_text + " " + (og_desc or "").lower())
    has_elevator = "asans" in full_text
    has_parking = "otopark" in full_text or "garaj" in full_text
    is_credit = "kredi" in full_text and "uygun" in full_text

    return {
        "url": url,
        "domain": "emlakjet.com",
        "source": "apify_emlakjet",
        "title": str(title)[:200],
        "price": price,
        "currency": currency,
        "city": city,
        "district": district,
        "neighborhood": neighborhood,
        "lat": "",
        "lon": "",
        "rooms": rooms,
        "grossM2": net_m2,
        "netM2": net_m2,
        "floor": floor,
        "buildingAge": "",
        "hasElevator": has_elevator,
        "hasParking": has_parking,
        "furnished": False,
        "isCreditEligible": is_credit,
        "images": images[:15],
        "description": description,
        "publishedDate": "",
    }
