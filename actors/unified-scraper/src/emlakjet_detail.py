"""
emlakjet_detail.py — Emlakjet.com tekil ilan URL scraper.

Chrome (Selenium) + __NEXT_DATA__ (Next.js) JSON çıkarımı.
İlan detay sayfasından TÜM alanları tek geçişte alır.
"""
from __future__ import annotations

import sys
import time
from pathlib import Path
from typing import Optional

# Kurulan konuma göre emlakjet-scraper kaynak dizinini ekle
# (Docker'da: /usr/src/app/scrapers/emlakjet, lokal: otomatik detect)
_EJ_SRC = Path("/usr/src/app/scrapers/emlakjet")
if not _EJ_SRC.exists():
    # Lokal geliştirme: sibling actors dizini
    _EJ_SRC = Path(__file__).parent.parent.parent / "emlakjet-scraper" / "src"
if str(_EJ_SRC) not in sys.path:
    sys.path.insert(0, str(_EJ_SRC))

import emlakjet_scraper as _ej  # noqa: E402  (path ayarlandı)

# ---------------------------------------------------------------------------
# Kapsamlı JS çıkarma — detail page __NEXT_DATA__ → tüm alanlar
# ---------------------------------------------------------------------------

_JS_DETAIL_FULL = r"""
return (function() {
    var res = {
        title: '', price: null, currency: 'TL',
        city: '', district: '', neighborhood: '',
        lat: '', lon: '',
        rooms: '', grossM2: null, netM2: null,
        floor: '', buildingAge: '',
        hasElevator: false, hasParking: false,
        furnished: false, isCreditEligible: false,
        images: [], description: '', publishedDate: '',
        is_jet_firsat: false,
        _ok: false
    };

    try {
        var nd = document.getElementById('__NEXT_DATA__');
        if (!nd) return res;
        var data = JSON.parse(nd.textContent);
        var pp = (data.props || {}).pageProps || {};

        // Detail page: ad / listing / adDetail / detail key dene
        var ad = pp.ad || pp.listing || pp.adDetail || pp.detail || pp.adDetails || {};
        if (!ad || !ad.title) return res;

        res._ok = true;
        res.title = String(ad.title || '');
        res.price  = ad.price != null ? Number(ad.price) : null;
        res.currency = String(ad.currency || 'TL');

        // Konum
        res.city         = String(((ad.city         || {}).name) || ad.cityName         || '');
        res.district     = String(((ad.county        || ad.district || {}).name) || ad.countyName || ad.districtName || '');
        res.neighborhood = String(((ad.neighborhood  || ad.mahalle || {}).name)  || '');

        // Koordinat
        var geo  = ad.coordinates || ad.geo || ad.location || {};
        res.lat  = String(ad.latitude  || ad.lat  || geo.lat  || geo.latitude  || '');
        res.lon  = String(ad.longitude || ad.lon  || ad.lng   || geo.lon || geo.lng || geo.longitude || '');

        // Oda: ["3","1"] → "3+1"
        var roomArr = ad.roomAndLivingRoom || [];
        res.rooms = roomArr.length ? roomArr.join('+') : String(ad.roomCount || '');

        // m²
        var sqm = ad.sqm || {};
        var grossArr = sqm.grossSqm || [];
        res.grossM2 = grossArr.length ? grossArr[0] : (ad.grossSqm != null ? ad.grossSqm : null);
        res.netM2   = sqm.netSqm  != null ? sqm.netSqm  : (ad.netSqm  != null ? ad.netSqm  : null);

        // Kat / yaş
        var floorObj = ad.floor || {};
        res.floor       = String(floorObj.name || ad.floorName || '');
        res.buildingAge = String(ad.age || ad.buildingAge || '');

        // Özellikler
        var inAttrs = ((ad.attributes || {}).inAttributes || []);
        var attrText = inAttrs.map(function(a){ return String(a.value || a.name || '').toLowerCase(); }).join(' ');
        res.hasElevator      = attrText.indexOf('asans') >= 0;
        res.hasParking       = attrText.indexOf('otopark') >= 0 || attrText.indexOf('garaj') >= 0;
        res.furnished        = ad.furnished === true;
        res.isCreditEligible = String(((ad.credit || {}).name) || '').toLowerCase().indexOf('uygun') >= 0;

        // Görseller
        res.images = (ad.images || []).map(function(img){
            return typeof img === 'string' ? img : String(img.url || img.src || img.original || '');
        }).filter(Boolean);

        res.description  = String(ad.detailDescription || ad.description || '');
        res.publishedDate = String(ad.createDate || ad.createdDate || ad.publishedDate || '');
        res.is_jet_firsat = !!(ad.isJetOpportunity || ad.isJetFirsat || false);

    } catch(e) {
        res._error = String(e);
    }
    return res;
})();
"""


def scrape_url(url: str) -> Optional[dict]:
    """Emlakjet.com ilan detay URL'sini scrape et. dict veya None döner."""
    driver = _ej.create_driver(headless=True, no_images=True)
    try:
        driver.get(url)

        # __NEXT_DATA__ hazır olana kadar bekle (maks 30s)
        try:
            from selenium.webdriver.support.ui import WebDriverWait
            WebDriverWait(driver, 30).until(
                lambda d: d.execute_script(
                    "return !!(document.getElementById('__NEXT_DATA__'))"
                )
            )
        except Exception:
            pass

        time.sleep(2.0)  # JS hydration tamamlanması için

        data = driver.execute_script(_JS_DETAIL_FULL)
        if not isinstance(data, dict) or not data.get("_ok"):
            return None

        data["url"]    = url
        data["domain"] = "emlakjet.com"
        data["source"] = "apify_emlakjet"
        return data

    except Exception as exc:
        print(f"[EMLAKJET_DETAIL] Hata ({url[:70]}): {exc}")
        return None
    finally:
        try:
            driver.quit()
        except Exception:
            pass
