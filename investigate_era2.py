"""Deep investigation of ERA Kadikoy listing page."""
import urllib.request
import re

UA = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

url = 'https://www.era.com.tr/kadikoy-satilik/konut'
req = urllib.request.Request(url, headers=UA)
try:
    with urllib.request.urlopen(req, timeout=12) as r:
        html = r.read().decode('utf-8', errors='replace')
        print(f'ERA: {r.status} {len(html):,} bytes -> {r.url[:80]}')
        all_hrefs = re.findall(r'href=["\']([^"\']+)["\']', html, re.I)
        print(f'All hrefs: {len(all_hrefs)}')
        # ERA listing link: ends with /\d+
        id_links = [h for h in all_hrefs if re.search(r'/\d{3,}$', h)]
        print(f'Numeric ID links: {len(id_links)}')
        for l in id_links[:10]:
            print(f'  {repr(l)}')
        # Look for the word kadikoy in the page
        if 'kadikoy' in html.lower():
            print('YES - kadikoy found in page')
        else:
            print('NO - kadikoy NOT in page')
        # Show a relevant section
        idx = html.lower().find('satilik')
        if idx > 0:
            print('Sample around first satilik:')
            print(repr(html[max(0,idx-100):idx+200]))
except Exception as e:
    print(f'ERROR: {e}')
