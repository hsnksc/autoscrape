"""Test search URLs for real-estate sites."""
import urllib.request
import re

UA = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
PAT = r'href=["\']([^"\']+satilik[a-z0-9\-]*/[a-z0-9\-]+/\d+)'

tests = [
    ('CB ?q',         'https://www.cb.com.tr/konut?q=kadikoy'),
    ('CB /il/ilce',   'https://www.cb.com.tr/satilik-konut/istanbul/kadikoy'),
    ('ERA ?q',        'https://www.era.com.tr/konut?q=kadikoy'),
    ('ERA slug',      'https://www.era.com.tr/istanbul-kadikoy-satilik/konut'),
    ('C21 ?q',        'https://www.century21.com.tr/konut?q=kadikoy'),
    ('C21 slug',      'https://www.century21.com.tr/istanbul-kadikoy-satilik/konut'),
    ('RealtyW q',     'https://www.realtyworld.com.tr/tr/portfoyler?query=kadikoy'),
    ('RealtyW il',    'https://www.realtyworld.com.tr/tr/portfoyler?il=istanbul&ilce=kadikoy'),
    ('Remax',         'https://www.remax.com.tr/konut/satilik/istanbul/kadikoy'),
]

for name, url in tests:
    try:
        req = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(req, timeout=10) as r:
            html = r.read().decode('utf-8', errors='replace')
            links = set(re.findall(PAT, html, re.I))
            print(f'{name}: HTTP {r.status} - {len(html):,} bytes - {len(links)} listing links')
            print(f'  URL: {r.url[:80]}')
            for lnk in list(links)[:3]:
                print(f'    {lnk[:80]}')
    except Exception as e:
        print(f'{name}: ERROR - {e}')
