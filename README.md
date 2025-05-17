# Stream_Processing_Wiki

## Introduction
The Wiki Analytics System collects, processes, and stores events of page creation from Wikimedia sites in real-time. It aggregating key metrics for analytics reporting via REST APIs.


## Data Flow
- Producer starts streaming messages from wiki_url to input_stream topic in kafka.
- Event arrives and is parsed into structured json format, and streamed to output_strem. For that Spark Streaming is used.
- Event is then consumed. Data is then inserted to Cassandra DB
- In-memory aggregation per domain(separated by bot and non-bot users) is done in mini batches in consumer. Every hour at 00m:00s aggregated stats are insert into Cassandra hourly_domain_stats. In-memory counters and maps are reset, for counting aggregated stats for new hour.
- REST API query → Returns stored aggregates instantly and queries data for tasks in category B.

## 3. Design Decisions
### 3.1 Data Store Choice: Cassandra
- Chosen for high write throughput and scalability.
- Supports time-series data modeling with clustering keys.

### 3.2 Data Modeling
- Separate tables for aggregated hourly stats and raw event data.
- Use primary keys to model partitions (domain, user_id...).
- Store statistics(domain, count) in a map data type for flexible metrics.
- See Cassandra Schema for more

### 3.3 Processing Strategy
Category A (1, 2 task):<br>
- Micro-Batch processing of hourly data to create reports.
- Real-time micro-batch approach: aggregates kept in-memory per hour, flushed to DB at hour boundaries.
- Precompute aggregates to optimize REST API query performance.
Category B and(task 3 Category A):<br>
- Cassandra Queries are made on demand by user



### How to run the application

1. **Clone the repository**

    ```bash
    git clone <repository-url>
    cd <repository-folder>
    ```

2. **Run docker compose:**

    ```bash
    docker-compose build --no-cache
    docker-compose up
    ```

3. 
    ```bash
    docker-compose down
    ```