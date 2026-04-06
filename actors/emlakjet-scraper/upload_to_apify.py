import json, requests, os, sys

base  = os.path.dirname(os.path.abspath(__file__))
token = "apify_api_nXCS0XsVftYxH3Ptiiy0qPKojn9ZAh0Bt83f"
actor_id = "8lU4k7t8LX1W8H9Zp"

files = [
    "Dockerfile",
    "requirements.txt",
    "README.md",
    ".actor/actor.json",
    ".actor/INPUT_SCHEMA.json",
    ".actor/OUTPUT_SCHEMA.json",
    "src/main.py",
    "src/emlakjet_scraper.py",
]

source_files = []
for f in files:
    path = os.path.join(base, f.replace("/", os.sep))
    with open(path, encoding="utf-8") as fh:
        source_files.append({"name": f, "format": "TEXT", "content": fh.read()})
    print(f"  ok: {f}")

payload = {"sourceType": "SOURCE_FILES", "sourceFiles": source_files}
r = requests.put(
    f"https://api.apify.com/v2/acts/{actor_id}/versions/0.1?token={token}",
    json=payload,
    timeout=60,
)
print(f"\nHTTP {r.status_code}")
data = r.json().get("data", {})
print("sourceType:", data.get("sourceType"))
print("files uploaded:", len(data.get("sourceFiles", [])))

if r.status_code != 200:
    print("ERROR:", r.text[:500])
    sys.exit(1)
