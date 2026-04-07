"""Deep investigation of ERA and CB listing pages for Kadıköy."""
import urllib.request
import re
from pathlib import Path

UA = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

# ERA Kadıköy listing page
tests = [
    ('ERA slug redirect', 'https://www.era.com.tr/kadikoy-satilik/konut'),
    ('ERA ALL links',     'https://www.era.com.tr/kadikoy-satilik/konut'),
    ('CB  kadikoy page2', 'https://www.cb.com.tr/kadikoy-satilik/daire?pager_p=2'),
    ('CB  istanbul konut','https://www.cb.com.tr/istanbul-satilik/konut'),
]

UA = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

for name, url in tests[:2]:
    req = urllib.request.Request(url, headers=UA)
    try:
        with urllib.request.urlopen(req, timeout=12) as r:
            html = r.read().decode('utf-8', errors='replace')
            filename = name.replace(' ', '_').replace('/', '_') + '.html'
            Path(filename).write_text(html[:10000])
            print(f'{name}: {r.status} {len(html):,} bytes -> {r.url[:70]}')
            # all hrefs
            all_hrefs = re.findall(r'href=["\']([^"\']+)["\']', html, re.I)
            print(f'  Total hrefs: {len(all_hrefs)}')
            # ERA specific: look for /something-satilik/type/id
            era_links = [h for h in all_hrefs if re.search(r'/\d{4,}$', h)]
            print(f'  Links ending with 4+ digit ID: {len(era_links)}')
            for l in era_links[:5]:
                print(f'    {l}')
    except Exception as e:
        print(f'{name}: ERROR - {e}')
    print()
