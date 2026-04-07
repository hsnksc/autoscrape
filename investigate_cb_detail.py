"""Test CB detail page HTML structure vs ERA pattern."""
import urllib.request
import re

UA = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

# Get a CB detail URL first
url = 'https://www.cb.com.tr/kadikoy-satilik/daire'
req = urllib.request.Request(url, headers=UA)
with urllib.request.urlopen(req, timeout=15) as r:
    html = r.read().decode('utf-8', errors='replace')
    links = set(re.findall(r'href=["\']([^"\']+(?:satilik|kiralik)[^"\']+/\d+)["\']', html, re.I))
    cb_url = 'https://www.cb.com.tr' + list(links)[0] if links else None
    print(f'CB listing: {list(links)[:2]}')
    print(f'Testing detail: {cb_url}')

if not cb_url:
    print('No CB url found!')
    exit()

# Fetch CB detail page
req2 = urllib.request.Request(cb_url, headers=UA)
with urllib.request.urlopen(req2, timeout=15) as r:
    detail_html = r.read().decode('utf-8', errors='replace')
    print(f'\nCB detail page: {r.status} {len(detail_html):,} bytes')
    
    # Check for ERA patterns
    checks = [
        ('googleMapOperations.lat', 'Koordinat (ERA pattern)'),
        ('<b>', 'Bold tags for features'),
        ('<tr', 'Table rows'),
        ('Fiyat</b>', 'Fiyat feature'),
        ('Oda Sayısı', 'Oda Sayısı feature'),
        ('Metre Kare', 'Metre Kare feature'),
        ('h1>', 'H1 tag'),
        ('Portföy No', 'Portfolio no'),
        ('Portfoy No', 'Portfolio no (ascii)'),
    ]
    for pattern, desc in checks:
        found = pattern.lower() in detail_html.lower()
        print(f'  [{" YES" if found else "  NO"}] {desc}: {pattern}')
    
    # Extract h1
    h1 = re.search(r'<h1[^>]*>(.*?)</h1>', detail_html, re.I | re.S)
    if h1:
        print(f'\nH1 title: {re.sub(r"<[^>]+>","",h1.group(1)).strip()[:80]}')

    # Check for feature tables
    rows = re.findall(r'<tr[^>]*>(.*?)</tr>', detail_html, re.I | re.S)
    print(f'\nTable rows: {len(rows)}')
    for row in rows[:5]:
        keys = re.findall(r'<b[^>]*>(.*?)</b>', row, re.I | re.S)
        vals = re.findall(r'<td[^>]*>(.*?)</td>', row, re.I | re.S)
        if keys and vals:
            k = re.sub(r'<[^>]+>', '', keys[0]).strip()
            v = re.sub(r'<[^>]+>', '', vals[-1]).strip()[:50] if vals else ''
            print(f'  {k}: {v}')
