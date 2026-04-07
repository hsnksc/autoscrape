import { ExaSearcher } from './src/orchestrator/exa-searcher.js';
const searcher = new ExaSearcher();
async function test() {
  const r = await searcher.search({
    location: 'Anadolu Yakasi, Kadikoy',
    rooms: '2+1',
    minPrice: 2000000,
    maxPrice: 5000000,
    lat: 40.9833,
    lng: 29.0333
  });
  const domains = r.linksOnly.map(u => new URL(u.url).hostname);
  const counts = {};
  for (const d of domains) counts[d] = (counts[d] || 0) + 1;
  console.log('linksOnly domains:', JSON.stringify(counts, null, 2));
  console.log('priced:', r.priced.length, 'linksOnly:', r.linksOnly.length);
  // Ilk 5 linksOnly URL
  console.log('First 5 linksOnly:');
  r.linksOnly.slice(0, 5).forEach(x => console.log(' -', x.url));
}
test().catch(console.error);
