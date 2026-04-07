import { ExaSearcher } from './src/orchestrator/exa-searcher.js';
const s = new ExaSearcher();
const r = await s.search({ location: 'Kadıköy', rooms: '2+1', minPrice: 2000000, maxPrice: 5000000 });
const allItems = [...r.priced, ...r.linksOnly];
const byHost = {};
for (const item of allItems) {
  try {
    const h = new URL(item.url).hostname;
    byHost[h] = (byHost[h] || 0) + 1;
  } catch {}
}
console.log('priced:', r.priced.length, 'linksOnly:', r.linksOnly.length, 'toplam:', allItems.length);
Object.entries(byHost).sort((a, b) => b[1] - a[1]).forEach(([h, c]) => console.log(c, h));
