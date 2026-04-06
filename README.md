# AutoScrape — Turkish Real Estate Search Engine

Powered by **Exa AI** (neural search) — no Apify, no Playwright, no residential proxy needed.

## Architecture

```
Client (React) ──POST /api/search──► Orchestrator (Express)
                                            │
                                     Exa AI API (neural search + text extract)
                                            │
                                 normalize → dedup → cache (Redis)
                                            │
Client (poll / WebSocket) ◄── results ◀─────┘
```

### Flow

1. User searches (location, rooms, price range)
2. Orchestrator creates a `jobId`, fires async Exa search
3. Exa returns listings with full page text — no browser needed
4. Backend normalizes → deduplicates by URL → caches to Redis
5. Client gets results via polling (`GET /api/job/:id`) or WebSocket

## Setup

### Prerequisites
- Node.js 20+
- Redis running locally
- Exa API key from https://exa.ai

### Install & Run

```bash
# 1. Install dependencies
npm install

# 2. Configure
cp .env.example .env
# Edit .env and set your EXA_API_KEY

# 3. Start backend
npm run dev          # → http://localhost:3001

# 4. Start client (separate terminal)
cd client && npm install && npm run dev  # → http://localhost:3000
```

### With Docker

```bash
docker compose up -d    # starts Redis + backend
```

## API Endpoints

### POST `/api/search`
```json
{
  "location": "Kadıköy",
  "rooms": "2+1",
  "minPrice": 2000000,
  "maxPrice": 5000000
}
```
Response: `{ "jobId": "uuid", "status": "searching" }`

### GET `/api/job/:jobId`
Response:
```json
{
  "jobId": "uuid",
  "status": "completed",
  "result": [ /* normalized listings */ ],
  "updatedAt": 1712345678
}
```

### WebSocket `/ws`
Connect and send `{"subscribe": "jobId"}` to receive real-time results.

## Listing Schema (normalized)
`url, domain, title, price, currency, city, district, rooms, netM2, grossM2, floor, buildingAge, isCreditEligible, hasElevator, hasParking, furnished, description, images[]`
