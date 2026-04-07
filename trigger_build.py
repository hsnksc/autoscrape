"""Apify actor build tetikleyici."""
import json, urllib.request, urllib.error

env = {}
for line in open('e:/autoscrape/.env'):
    line = line.strip()
    if '=' in line and not line.startswith('#'):
        k, _, v = line.partition('=')
        env[k.strip()] = v.strip()

token = env['APIFY_TOKEN']
actor_id = 'ezXC26HpAu0r244km'

# Query parametreleri ile build tetikle
url = f'https://api.apify.com/v2/acts/{actor_id}/builds?token={token}&version=1.0&tag=latest&useCache=false'
req = urllib.request.Request(url, data=b'{}', method='POST', headers={'Content-Type': 'application/json'})
try:
    with urllib.request.urlopen(req, timeout=20) as r:
        body = json.loads(r.read())
        d = body['data']
        print(f'Build OK: id={d["id"]} num={d["buildNumber"]} status={d["status"]}')
        print(f'Build URL: https://console.apify.com/actors/{actor_id}/builds/{d["id"]}')
except urllib.error.HTTPError as e:
    print(f'HTTP ERROR: {e.code} {e.read().decode()[:500]}')
