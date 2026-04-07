"""Test location-based search URLs for CB, ERA, Century21, RealtyWorld."""
import urllib.request
import re

UA = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

def count_listings(html: str) -> int:
    return len(set(re.findall(r'href=["\']([^"\']+(?:satilik|kiralik)[^"\']+/\d+)["\']', html, re.I)))

tests = [
    ('CB slug-city',     'https://www.cb.com.tr/istanbul-kadikoy-satilik/daire/'),
    ('CB slug2',         'https://www.cb.com.tr/istanbul-kadikoy-satilik/konut/'),
    ('CB il-param',      'https://www.cb.com.tr/konut?il=istanbul&ilce=kadikoy'),
    ('CB city-path',     'https://www.cb.com.tr/konut/istanbul/kadikoy'),
    ('ERA slug',         'https://www.era.com.tr/istanbul-kadikoy-satilik/konut/'),
    ('ERA il-param',     'https://www.era.com.tr/konut?il=istanbul&ilce=kadikoy'),
    ('RealtyW il',       'https://www.realtyworld.com.tr/tr/portfoyler?il=istanbul&ilce=kadikoy'),
    ('RealtyW search',   'https://www.realtyworld.com.tr/tr/portfoyler?ilce=Kadikoy'),
    ('RealtyW tur',      'https://www.realtyworld.com.tr/tr/portfoyler?Ilce=Kadikoy&Tip=1'),
]

for name, url in tests:
    try:
        req = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(req, timeout=12) as r:
            html = r.read().decode('utf-8', errors='replace')
            c = count_listings(html)
            fin = r.url
            redir = ' [REDIR]' if fin != url else ''
            print(f'{name}: {r.status} - {len(html):,} bytes - {c} listings{redir}')
            if c > 0:
                links = set(re.findall(r'href=["\']([^"\']+(?:satilik|kiralik)[^"\']+/\d+)["\']', html, re.I))
                for lnk in list(links)[:3]:
                    print(f'  {lnk}')
            if fin != url:
                print(f'  -> {fin[:80]}')
    except Exception as e:
        print(f'{name}: ERROR - {str(e)[:60]}')
