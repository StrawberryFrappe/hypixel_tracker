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
