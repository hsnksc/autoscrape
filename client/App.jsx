import { useState, useCallback } from 'react';
import SearchForm from './components/SearchForm';
import StatusBar from './components/StatusBar';
import ResultsList from './components/ResultsList';

const API_BASE = import.meta.env.VITE_API_URL || 'http://localhost:3001';

export default function App() {
  const [jobId, setJobId] = useState(null);
  const [status, setStatus] = useState(null);
  const [results, setResults] = useState([]);
  const [error, setError] = useState(null);
  const [isSearching, setIsSearching] = useState(false);

  // Start a search job
  const onSearch = async (criteria) => {
    setError(null);
    setResults([]);
    setIsSearching(true);

    try {
      const res = await fetch(`${API_BASE}/api/search`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(criteria),
      });

      if (!res.ok) {
        const err = await res.json();
        throw new Error(err.error || 'Search failed');
      }

      const data = await res.json();
      setJobId(data.jobId);
      setStatus(data.status);
      pollJob(data.jobId);
    } catch (err) {
      setError(err.message);
      setIsSearching(false);
    }
  };

  // Poll GET /api/job/:jobId every 3s
  const pollJob = useCallback((id) => {
    const interval = setInterval(async () => {
      try {
        const res = await fetch(`${API_BASE}/api/job/${id}`);
        if (!res.ok) return;

        const data = await res.json();
        setStatus(data.status);

        if (data.status === 'completed' && data.result) {
          setResults(data.result);
          setIsSearching(false);
          clearInterval(interval);
        } else if (data.status === 'failed') {
          setError(data.error || 'Job failed');
          setIsSearching(false);
          clearInterval(interval);
        }
      } catch {
        // network error — keep polling
      }
    }, 3000);

    return () => clearInterval(interval);
  }, []);

  return (
    <div className="app">
      <h1>AutoScrape</h1>
      <p className="subtitle">Türk emlak sitelerinden toplu ilan arama</p>

      <SearchForm onSubmit={onSearch} disabled={isSearching} />

      {status && (
        <StatusBar status={status} />
      )}

      {error && <p className="error-msg">{error}</p>}

      {results.length > 0 && <ResultsList items={results} />}
    </div>
  );
}