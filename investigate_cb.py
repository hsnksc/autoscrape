"""Investigate CB and ERA listing page HTML structure."""
import urllib.request
import re
from pathlib import Path

UA = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

# Test CB konut listing page
url = 'https://www.cb.com.tr/konut'
req = urllib.request.Request(url, headers=UA)
with urllib.request.urlopen(req, timeout=15) as r:
    html = r.read().decode('utf-8', errors='replace')
    print(f'CB /konut: {r.status} - {len(html):,} bytes')
    print(f'Final URL: {r.url}')
    # Find all href patterns
    all_hrefs = re.findall(r'href=["\']([^"\']{10,80})["\']', html, re.I)
    # Look for listing links
    listing_hrefs = [h for h in all_hrefs if re.search(r'satilik|kiralik', h, re.I) and re.search(r'/\d+$', h)]
    print(f'Total hrefs: {len(all_hrefs)}, Listing hrefs: {len(listing_hrefs)}')
    if listing_hrefs:
        print('Sample listing hrefs:')
        for lnk in listing_hrefs[:10]:
            print(f'  {lnk}')
    
    # Check pagination pattern
    pages = re.findall(r'pager_p=(\d+)', html)
    print(f'Pagination numbers found: {sorted(set(int(x) for x in pages))[:10]}')
    
    # Save a snippet for investigation
    Path('cb_snippet.html').write_text(html[:5000], encoding='utf-8')
    print('Saved first 5000 chars to cb_snippet.html')
