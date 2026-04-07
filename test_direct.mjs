/**
 * Test DirectSearcher in isolation
 */
import { DirectSearcher } from './src/orchestrator/direct-searcher.js';

const searcher = new DirectSearcher();

console.log('Testing DirectSearcher for Kadıköy...');
const urls = await searcher.searchAll('Kadıköy');
console.log(`Found ${urls.length} URLs`);
urls.slice(0, 10).forEach(u => console.log(' ', u));
