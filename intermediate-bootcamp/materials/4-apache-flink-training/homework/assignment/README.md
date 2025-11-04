# Apache Flink Sessionization Assignment

## Overview
This assignment implements a sessionization pipeline using Apache Flink that:
- Reads web traffic events from Kafka
- Creates sessions with 5-minute gaps per IP/host combination  
- Writes session aggregates to PostgreSQL
- Uses 15-second watermark tolerance for late events

## Prerequisites

### Environment Variables
Set the following environment variables:

```bash
export POSTGRES_URL="jdbc:postgresql://localhost:5432/postgres"
export POSTGRES_USER="postgres" 
export POSTGRES_PASSWORD="postgres"
export KAFKA_URL="your-kafka-bootstrap-servers"
export KAFKA_TOPIC="your-topic-name"
export KAFKA_GROUP="your-consumer-group"
export KAFKA_WEB_TRAFFIC_KEY="your-kafka-key"
export KAFKA_WEB_TRAFFIC_SECRET="your-kafka-secret"
```

## Running the Pipeline

### 1. Initialize Database
Run the initialization script to create required tables:

```bash
psql -h localhost -U postgres -d postgres -f init.sql
```

### 2. Start Services
Start Kafka, PostgreSQL, and Flink cluster:

```bash
make up
```

### 3. Submit Flink Job
Submit the sessionization job:

```bash
make sessionization_job
```

### 4. Validate Results
Query the results and run analytics:

```bash
# Check session data is being written
psql -h localhost -U postgres -d postgres -c "SELECT COUNT(*) FROM processed_events_aggregated_session;"

# Run analytics queries  
psql -h localhost -U postgres -d postgres -f avg_session_events.sql
```

## Expected Results

Based on the assignment data, you should see:

1. **Average events per session across Tech Creator**: 2.89
2. **Average per user (IP) over their sessions on Tech Creator**: 2.16  
3. **Host comparison**:
   - "lulu.techcreator.io": 2.0
   - "zachwilson.techcreator.io": 1.5

## Architecture Notes

- **Session Window**: 5-minute gap between events per IP/host
- **Watermark**: 15-second tolerance (events arriving >15s late will be dropped)
- **Checkpointing**: Enabled with 60-second interval
- **Parallelism**: Set to 3
- **Buffering**: JDBC sink buffers up to 1000 rows or 2-second intervals

## Troubleshooting

- Ensure `init.sql` runs before starting the job
- Verify Kafka topic is created and producing events
- Check Flink logs: `docker-compose logs jobmanager`
- Validate environment variables are set correctly