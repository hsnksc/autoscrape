"""Son Apify run logunu getir ve hepsiemlak/sahibinden satırlarını yazdır."""
import json, urllib.request

env = {}
for line in open('e:/autoscrape/.env'):
    line = line.strip()
    if '=' in line and not line.startswith('#'):
        k, _, v = line.partition('=')
        env[k.strip()] = v.strip()

token = env['APIFY_TOKEN']
actor_id = 'ezXC26HpAu0r244km'

# Son run
req = urllib.request.Request(f'https://api.apify.com/v2/acts/{actor_id}/runs?token={token}&limit=1&desc=true')
with urllib.request.urlopen(req, timeout=15) as r:
    body = json.loads(r.read())
    run = body['data']['items'][0]
    run_id = run['id']
    status = run['status']
    build_num = run['buildNumber']
    print(f'Son run: {run_id} | status={status} | build={build_num}')

# Log
log_url = f'https://api.apify.com/v2/logs/{run_id}?token={token}'
req2 = urllib.request.Request(log_url)
with urllib.request.urlopen(req2, timeout=20) as r:
    log = r.read().decode('utf-8', errors='replace')

lines = log.splitlines()
keywords = ['HEPSIEMLAK', 'SAHIBINDEN', 'PLAYWRIGHT', 'DOM', 'Error', 'ERROR', 'Traceback', 'exception', 'Import']
for line in lines:
    if any(k.lower() in line.lower() for k in keywords):
        print(line)
