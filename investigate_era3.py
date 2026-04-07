"""Show full ERA page content for analysis."""
import urllib.request
import re

UA = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

url = 'https://www.era.com.tr/kadikoy-satilik/konut'
req = urllib.request.Request(url, headers=UA)
with urllib.request.urlopen(req, timeout=15) as r:
    html = r.read().decode('utf-8', errors='replace')

# Show all hrefs
hrefs = re.findall(r'href=["\']([^"\']+)["\']', html, re.I)
print('=== ALL HREFS ===')
for h in hrefs:
    print(h)

print('\n=== LOOK FOR DATA PATTERNS ===')
# Find any pattern that could be listing data
for pat in [r'"url"\s*:\s*"([^"]+)"', r'"link"\s*:\s*"([^"]+)"', r'portfoy_no.*?(\d+)', r'/konut/\d+', r'/daire/\d+']:
    matches = re.findall(pat, html, re.I)
    if matches:
        print(f'Pattern {pat}: {len(matches)} matches')
        for m in matches[:3]:
            print(f'  {m}')

print('\n=== SCRIPTS/AJAX CLUES ===')
scripts = re.findall(r'<script[^>]*src=["\']([^"\']+)["\']', html, re.I)
for s in scripts[:10]:
    print(s)

# Look for json data
json_inline = re.findall(r'\{[^{}]{100,500}\}', html)
print(f'\nInline JSON-like blocks: {len(json_inline)}')
for j in json_inline[:3]:
    print(j[:200])
