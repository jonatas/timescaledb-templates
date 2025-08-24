# Webtop - Website Traffic Pipeline

A high-performance TimescaleDB pipeline for tracking and analyzing website traffic patterns.

## Features

- Real-time website traffic monitoring
- Hierarchical continuous aggregates (1m, 1h, 1d)
- Top websites election system
- Configurable retention policies
- High-throughput data processing

## Quick Start

### Prerequisites

- Docker and Docker Compose
- At least 8GB RAM recommended

### Data Persistence

The database data is automatically persisted using Docker volumes. Your data will survive container restarts and updates.

**Important**: The database is automatically initialized with the basic schema on first startup. For full setup with data generation, use the setup script.

### Starting the System

```bash
# Start the database
docker-compose up -d

# Check status
docker-compose ps
```

### Database Management

Use the provided setup script for database operations:

```bash
./setup-database.sh
```

Options:
- **Basic setup**: Creates tables and continuous aggregates
- **Full setup**: Includes data generation and scheduling
- **Reset**: Clears all data and resets to initial state
- **Status check**: Shows current database state

### Manual Database Access

```bash
# Connect to database
docker exec -it timescaledb psql -U postgres -d pipeline

# Check tables
\dt

# Check continuous aggregates
SELECT view_name FROM timescaledb_information.continuous_aggregates;
```

## Configuration

The system uses the following default configuration:
- Database: `pipeline`
- User: `postgres`
- Password: `password`
- Port: `5432`

## Data Retention

- Logs: 5 minutes (for demo purposes)
- Candidates: 7 days
- Long-term storage: 90 days
- Max entries: 1000

## Troubleshooting

### Data Loss Issues

If you experience data loss:

1. Check if the volume exists:
   ```bash
   docker volume ls | grep timescaledb
   ```

2. Verify volume data:
   ```bash
   docker volume inspect webtop_timescaledb_data
   ```

3. Restart with data preservation:
   ```bash
   docker-compose down
   docker-compose up -d
   ```

### Container Won't Start

1. Check logs:
   ```bash
   docker-compose logs timescaledb
   ```

2. Ensure sufficient resources (8GB+ RAM)

3. Check port availability (5432)

## Architecture

- **TimescaleDB**: Time-series database with PostgreSQL
- **Hypertables**: Automatic partitioning by time
- **Continuous Aggregates**: Pre-computed aggregations
- **Background Jobs**: Automated data processing

## Performance

Designed for high-throughput scenarios:
- Target: ~20M records/minute
- Retention: Configurable per use case
- Elections: Every 5 minutes (configurable)



