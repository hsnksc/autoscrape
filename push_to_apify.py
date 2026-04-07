"""Push updated source files to Apify actor using the API."""
import json
import sys
import urllib.request
import urllib.parse
from pathlib import Path

# Read .env for token
env = {}
for line in Path('e:/autoscrape/.env').read_text().splitlines():
    if '=' in line and not line.startswith('#'):
        k, _, v = line.partition('=')
        env[k.strip()] = v.strip()

TOKEN = env.get('APIFY_TOKEN', '')
ACTOR_ID = 'ezXC26HpAu0r244km'
VERSION = '1.0'

if not TOKEN:
    print('ERROR: APIFY_TOKEN not found')
    sys.exit(1)

def read_file(path):
    return Path(path).read_text(encoding='utf-8')

# Read all current source files
BASE = 'e:/autoscrape/actors/unified-scraper'
files = {
    'requirements.txt':           BASE + '/requirements.txt',
    'Dockerfile':                  BASE + '/Dockerfile',
    '.actor/actor.json':           BASE + '/.actor/actor.json',
    '.actor/input_schema.json':    BASE + '/.actor/input_schema.json',
    'src/emlakjet_detail.py':      BASE + '/src/emlakjet_detail.py',
    'src/hepsiemlak_detail.py':    BASE + '/src/hepsiemlak_detail.py',
    'src/sahibinden_detail.py':    BASE + '/src/sahibinden_detail.py',
    'src/remax_detail.py':         BASE + '/src/remax_detail.py',
    'src/playwright_fetch.py':     BASE + '/src/playwright_fetch.py',
    'src/generic_detail.py':       BASE + '/src/generic_detail.py',
    'src/shb_detail.py':           BASE + '/src/shb_detail.py',
    'src/normalize.py':            BASE + '/src/normalize.py',
    'src/main.py':                 BASE + '/src/main.py',
}

source_files = []
for name, path in files.items():
    try:
        content = read_file(path)
        source_files.append({'name': name, 'format': 'TEXT', 'content': content})
        print(f'  + {name} ({len(content)} chars)')
    except FileNotFoundError:
        print(f'  ! MISSING: {name} -> {path}')

# Read scrapers (these stay unchanged)
scrapers = [
    ('scrapers/emlakjet/emlakjet_scraper.py', BASE + '/scrapers/emlakjet/emlakjet_scraper.py'),
    ('scrapers/hepsiemlak/hepsiemlak_scraper.py', BASE + '/scrapers/hepsiemlak/hepsiemlak_scraper.py'),
    ('scrapers/hepsiemlak/scraper_base.py', BASE + '/scrapers/hepsiemlak/scraper_base.py'),
    ('scrapers/sahibinden/sahibinden_scraper.py', BASE + '/scrapers/sahibinden/sahibinden_scraper.py'),
]
for name, path in scrapers:
    try:
        content = read_file(path)
        source_files.append({'name': name, 'format': 'TEXT', 'content': content})
        print(f'  + {name} ({len(content)} chars)')
    except FileNotFoundError:
        # These scrapers existed on Apify but may not be local — skip
        print(f'  ~ SKIP (not local): {name}')

payload = json.dumps({'sourceFiles': source_files}).encode('utf-8')
url = f'https://api.apify.com/v2/acts/{ACTOR_ID}/versions/{VERSION}?token={TOKEN}'

print(f'\nUpdating {len(source_files)} files...')
req = urllib.request.Request(url, data=payload, method='PUT',
    headers={'Content-Type': 'application/json'})
try:
    with urllib.request.urlopen(req, timeout=30) as resp:
        result = json.loads(resp.read())
        print(f'SUCCESS: {resp.status}')
        if 'data' in result:
            print(f'Version: {result["data"].get("versionNumber")}')
            print(f'Source files: {len(result["data"].get("sourceFiles", []))}')
except urllib.error.HTTPError as e:
    body = e.read().decode('utf-8', errors='replace')
    print(f'ERROR HTTP {e.code}: {body[:500]}')
