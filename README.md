# Hypixel Bazaar Tracker

A local tracking system for the Hypixel Bazaar, consisting of a data scraper, PostgreSQL database, and an API for querying historical data.

## Architecture

The project is composed of the following Docker services:

- **postgres**: PostgreSQL database for storing bazaar data.
- **fetcher**: Python service that fetches data from the Hypixel API and stores it in the database.
- **sql_processor**: Processes raw data into optimized tables.
- **api**: FastAPI service exposing endpoints to query the data.
- **adminer**: Database management interface.

## Getting Started

### Prerequisites

- Docker and Docker Compose installed.

### Running the Project

You can start all services using the provided script:

```bash
./deploy.sh
```

Or manually with Docker Compose:

```bash
docker compose up -d --build
```

### Accessing Services

- **API**: http://localhost:8001
- **Adminer**: http://localhost:8080 (System: PostgreSQL, Server: postgres, Username: user, Password: password, Database: bazaar_data)

## Development

The project is structured as follows:

- `api/`: FastAPI application code.
- `scraper/`: Data fetching and processing logic.
- `docker-compose.yml`: Service definitions.

## API Endpoints

The API runs on port `8001` by default.

### General

- `GET /health`: Health check.
- `GET /latest`: Returns the latest processed bazaar data snapshot.

### Products

All product endpoints support the following query parameters:
- `start` (optional): Start timestamp (Unix milliseconds) or ISO 8601 date string (e.g., `2023-10-27T10:00:00`).
- `end` (optional): End timestamp (Unix milliseconds) or ISO 8601 date string.
- `lookback` (optional): Duration to look back from `end` (or now if `end` is not provided). Format: `MM:DD:HH:MM:SS` (Months:Days:Hours:Minutes:Seconds).
- `limit` (optional): Maximum number of records to return (default: 100, max: 1000).

#### Endpoints

- `GET /products/{product_id}/status`: Get historical status (prices, volumes, etc.) for a specific product.
- `GET /products/{product_id}/buy-offers`: Get historical buy offers for a specific product.
- `GET /products/{product_id}/sell-offers`: Get historical sell offers for a specific product.

### Example Usage

```bash
# Get status history for Enchanted Iron Ingot (last 5 records)
curl "http://localhost:8001/products/ENCHANTED_IRON_INGOT/status?limit=5"

# Get status history for a specific date range
curl "http://localhost:8001/products/ENCHANTED_IRON_INGOT/status?start=2023-10-27T00:00:00&end=2023-10-28T00:00:00"

# Get status history for the last 24 hours
curl "http://localhost:8001/products/ENCHANTED_IRON_INGOT/status?lookback=00:00:24:00:00"
```
