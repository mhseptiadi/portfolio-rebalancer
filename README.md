# Portfolio Rebalancer

This is a take-home assignment to build backend APIs for managing and rebalancing user portfolios.


## Tech Stack

- Go
- Elasticsearch (Feel free to use any other database or an in-memory alternative)
- Kafka (Feel free to use any other messaging system if needed)
- Docker


## Architecture choices

### Why Kafka KRaft?

This stack uses Kafka in **KRaft** mode (Kafka Raft metadata): **no ZooKeeper**, **faster broker startup**, and a metadata layer designed for **stronger fault tolerance** and simpler operations than the legacy ZooKeeper deployment model.

### Why split the API and the worker?

The **HTTP API** and **Kafka consumers** run as **separate services** so you can **scale consumer throughput independently** from request-handling capacity. Rebalance work can grow with topic partitions and worker replicas without forcing the same scaling shape as public API traffic.


## Running the Project

Docker Compose starts Elasticsearch, Kafka, Kafka UI, the **API** container, and **worker** containers (API and worker are separate images/services). The HTTP API listens on **port 8083** so it does not conflict with another service using 8080 on your machine.

```bash
docker compose build
docker compose up
```

The API is available at `http://localhost:8083`.

**Kafka and workers** (edit `docker-compose.yml` to tune):

- **Partitions:** Kafka is configured with `KAFKA_NUM_PARTITIONS` (default **10**) on the `kafka` service. Change that value if you want a different default partition count for new topics.
- **Workers:** The `worker` service uses `deploy.replicas` (default **3**). That controls how many worker containers run.
- **Constraint:** Keep the number of **worker replicas less than or equal to** the topic’s partition count. If you scale workers up, raise `KAFKA_NUM_PARTITIONS` (or ensure the rebalance topic has enough partitions) so you do not end up with more consumers than partitions—extra consumers would stay idle and you lose the expected parallelism model.


## Local development (Air)

For live reload while you edit Go code, you can run **[Air](https://github.com/air-verse/air)** for the API and worker, and keep Elasticsearch and Kafka on Docker.

1. Install Air: `go install github.com/air-verse/air@latest`
2. Start only the dependencies (skip the `api` and `worker` services so ports and processes are free):

   ```bash
   docker compose up elasticsearch kafka kafka-ui
   ```

3. From the **repository root**, point the apps at the host-mapped broker and Elasticsearch (`docker-compose.yml` exposes Kafka on **29092** for clients outside the Compose network):

   ```bash
   export KAFKA_BROKERS=localhost:29092
   export ELASTICSEARCH_URL=http://localhost:9200
   ```

4. **API** (uses `.air.toml`):

   ```bash
   air
   ```

5. **Worker** in a second terminal (uses `.air.worker.toml`):

   ```bash
   air -c .air.worker.toml
   ```

The API serves on **http://localhost:8083** (same as the containerized setup). Binaries are written under `tmp/` while Air runs.


## Running tests

```bash
go test ./...
```


## Models

- Portfolio 
        - `UserID` field is a unique user identifier in our system
        - `Allocation` field represents the percentage of the user's total portfolio or cash distribution across different asset classes. 
            Eg: {"stocks": 60, "bonds": 30, "gold": 10}.
            Note: This means 60% of the user's portfolio is allocated to stocks, 30% to bonds, and 10% to gold
            
- UpdatedPortfolio 
        - `UserID` is the user's unique ID
        - `NewAllocation` is the new allocation of user portfolio in %.

- RebalanceTransaction
        - `userID` is the user's unique ID
        - `Action` is the type of transaction (BUY/SELL)
        - `Asset` is the type of user asset to be transferred (eg: stocks, bonds, gold etc.)
        - `RebalancePercent` is the percentage of the asset transferred


## APIs

Base URL: `http://localhost:8083`

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/portfolio` | Create a portfolio. JSON body: `user_id`, `allocation` (`stocks`, `bonds`, `gold` as percentages summing to 100). Returns `201` with the saved portfolio. |
| `GET` | `/portfolio/id` | Get one portfolio. Query: `user_id` (required). |
| `GET` | `/portfolios` | List all portfolios. |
| `POST` | `/rebalance` | Enqueue a rebalance. JSON body: `user_id`, `new_allocation` (same shape as allocation). Publishes to Kafka; workers apply the rebalance, persist transactions, and update the portfolio. Returns `200` when the message is published. |
| `GET` | `/transactions` | List rebalance transactions for a user. Query: `user_id` (required). |


## TODO

- **`internal/handlers/portfolio.go`** (around line 238, `HandleRebalanceMessage`): transactions and portfolio should be saved in a **single session** (atomic unit of work) to avoid **partial updates** if one of the writes fails after others succeed.


## Example

- `/portfolio` API creates a user with ID = 1 and Allocation = {"stocks": 60, "bonds": 30, "gold": 10}
    Note: here the allocation is 60% stocks, 30% bonds and 10% gold

- `/rebalance` API is called with inputs
    ID = 1
    NewAllocation = {"stocks": 70, "bonds": 20, "gold": 10}
    [This is how much the user's portfolio has moved due to market conditions]

- We need to calculate and save the RebalanceTransaction to maintain 60% stocks, 30% bonds and 10% gold.

    Transaction 1: 
            UserID = "1"
	        Sell 10% of stocks

    Transaction 2: 
            UserID = "1"
	        Buy 10% of bonds

