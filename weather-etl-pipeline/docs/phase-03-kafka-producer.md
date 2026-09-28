# Phase 3: Kafka setup and producer

## 1. Objective
Start Kafka in Docker, create the `weather-data` topic, and publish every weather event to it reliably, knowing for each message whether the broker actually stored it.

## 2. Architecture position
```
[Weather API] ──► [Python ingestion] ──► [Kafka: weather-data]
                          ▲ producer        ▲ broker
```

## 3. Prerequisites
* Phase 2 works (`--dry-run` prints events)
* Docker Desktop is **running** (whale icon in the system tray)

## 4. Folder / file structure
```
docker/docker-compose.yml     Kafka + Kafka UI (+ Spark later)
ingestion/kafka_producer.py   WeatherProducer class          ◄ this phase
ingestion/main.py             now publishes instead of printing
```

## 5. Commands

```powershell
# --- Start Kafka + Kafka UI -------------------------------------------
cd docker
docker compose up -d
docker compose ps            # wait until kafka shows "(healthy)", about 20-30s

# --- Create the topic (once) ------------------------------------------
docker exec weather-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 `
  --create --topic weather-data --partitions 3 --replication-factor 1

# --- Check it ---------------------------------------------------------
docker exec weather-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
docker exec weather-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 `
  --describe --topic weather-data

# --- Run the producer (from the project root) --------------------------
cd ..
python -m ingestion.main --once          # one cycle
python -m ingestion.main                 # run continuously (Ctrl+C to stop)
python -m ingestion.main --mock          # continuous, fake data
```

Kafka UI: open **http://localhost:8080**, then **Topics → weather-data → Messages**.

## 6. Code

**docker-compose.yml (Kafka part)**. One container in KRaft mode, no ZooKeeper:
```yaml
KAFKA_LISTENERS: HOST://0.0.0.0:9092,DOCKER://0.0.0.0:29092,CONTROLLER://0.0.0.0:9093
KAFKA_ADVERTISED_LISTENERS: HOST://localhost:9092,DOCKER://kafka:29092
KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
KAFKA_LOG_RETENTION_HOURS: 168
```

**ingestion/kafka_producer.py**
```python
self.producer = Producer({
    "bootstrap.servers": bootstrap_servers,
    "acks": "all",                 # broker confirms the write
    "enable.idempotence": True,    # no duplicates from producer retries
    "linger.ms": 50,
})

def publish(self, event):
    self.producer.produce(self.topic,
                          key=event["city"].encode("utf-8"),        # same city -> same partition
                          value=json.dumps(event).encode("utf-8"),
                          on_delivery=self._on_delivery)            # success/failure callback
    self.producer.poll(0)
```
`check_connection()` runs at startup and fails fast if Kafka is down or the topic is missing.

## 7. Explanation

**Two listeners, why?** Your Python code runs on Windows and reaches Kafka through the published port `localhost:9092`. Kafka UI and Spark run *inside* Docker, where `localhost` means "this container", so they use `kafka:29092`. Kafka tells each client which address to use next (the *advertised* listener). With only one listener, one of the two sides would always fail. This is the #1 Kafka-in-Docker problem.

**`acks=all` + idempotence.** `produce()` is asynchronous: it only queues the message. The broker's confirmation arrives later through `on_delivery`. We count confirmed deliveries and failures, and `flush()` at the end of every cycle waits for all confirmations. So `delivered=7` in the summary means Kafka really has them.

**Why the city as key?** Kafka hashes the key to pick a partition. The same key always goes to the same partition, and Kafka guarantees order *within* a partition. So Delhi's readings are always in time order.

**Why auto topic creation is off.** With it on, a typo in `KAFKA_TOPIC` silently creates a new, empty topic and your consumer waits forever. With it off, you get a clear error.

## 8. Expected output
```
2026-09-28 08:52:18,500 INFO    ingestion - Connected to Kafka at localhost:9092, topic=weather-data
2026-09-28 08:52:18,500 INFO    ingestion - Polling 7 cities every 300s
2026-09-28 08:52:18,500 INFO    ingestion - Fetched Delhi      temp= 25.6C humidity=66% condition=Rain
...
2026-09-28 08:52:19,036 INFO    ingestion - CYCLE 1 SUMMARY cities=7 fetched=7 api_failures=0 invalid=0 published=7 delivered=7 delivery_failed=0 pending=0 duration=0.5s
```
You may see one harmless line on the very first run after starting Kafka:
`Failed to acquire idempotence PID ... Coordinator load in progress: retrying`. The broker is still warming up and the client retries by itself.

## 9. Testing steps
1. `delivered` equals `published` in the CYCLE SUMMARY.
2. Offsets grow by 7 per cycle (total across the 3 partitions):
   ```powershell
   docker exec weather-kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic weather-data
   # weather-data:0:3   weather-data:1:3   weather-data:2:1   (sum = 7 after one cycle)
   ```
3. **Kafka down test:** `docker compose stop kafka`, then run the producer. It should exit immediately with `Cannot reach Kafka ... Is 'docker compose up -d' running?`. Start Kafka again.
4. **Missing topic test:** set `KAFKA_TOPIC=weather-dataa` in `.env`. You should get `topic 'weather-dataa' does not exist`. Revert.

## 10. Common errors and fixes

| Error | Fix |
|---|---|
| `Cannot reach Kafka` / `Connection refused` | `docker compose ps`: is kafka running and healthy? Is `KAFKA_BOOTSTRAP_SERVERS=localhost:9092`? |
| Producer connects, then `kafka:29092: Failed to resolve` | Your `.env` has `kafka:29092`. On Windows it must be `localhost:9092`. |
| `port is already allocated` for 9092/8080 | Another program is using the port. Stop it, or change the left-hand port in compose (e.g. `"9094:9092"`). The advertised `HOST://localhost:9092` must then match. |
| Kafka container exits immediately | `docker logs weather-kafka`. Often corrupt data after an unclean shutdown: `docker compose down -v` (deletes topic data) and start again. |
| `TopicExistsException` | The topic was already created. That's fine. |

## 11. Verify before moving on
- [ ] `docker compose ps` shows kafka **healthy** and kafka-ui up
- [ ] The topic exists with 3 partitions
- [ ] The CYCLE SUMMARY shows `delivered=7 delivery_failed=0`
- [ ] Messages are visible in Kafka UI
