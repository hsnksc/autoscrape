// Exa per-domain test: era/remax/century21/turyap/realtyworld için ayrı sorgu
const EXA_KEY = process.env.EXA_API_KEY;
const domains = ['era.com.tr', 'remax.com.tr', 'century21.com.tr', 'turyap.com.tr', 'realtyworld.com.tr'];

for (const domain of domains) {
  const body = {
    query: 'Kadıköy 2+1 satılık daire ilan',
    type: 'neural',
    numResults: 5,
    includeDomains: [domain],
    contents: { text: false },
  };
  const res = await fetch('https://api.exa.ai/search', {
    method: 'POST',
    headers: { 'x-api-key': EXA_KEY, 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  const data = await res.json();
  const count = data.results?.length ?? 0;
  const urls = data.results?.map(r => r.url).slice(0, 2) ?? [];
  console.log(`${domain}: ${count} sonuç - ${urls[0] ?? 'YOK'}`);
}
