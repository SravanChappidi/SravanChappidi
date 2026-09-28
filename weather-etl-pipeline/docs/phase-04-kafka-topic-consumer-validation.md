# Phase 4: Kafka topic and consumer validation

## 1. Objective
Understand the Kafka concepts you're using, and **prove** from the consumer side that events are in Kafka, complete, correctly keyed and readable, before Spark gets involved.

## 2. Architecture position
```
[Python producer] ──► [Kafka: weather-data] ──► [validation consumer]   (Spark in Phase 5)
```

## 3. Prerequisites
Phase 3: Kafka running, topic created, producer has sent at least one cycle.

## 4. Folder / file structure
```
scripts/consume_test.py    Python consumer for validation     ◄ this phase
```

## 5. Commands

```powershell
# A. Kafka's own console consumer: read everything from the start, print keys
docker exec -it weather-kafka /opt/kafka/bin/kafka-console-consumer.sh `
  --bootstrap-server localhost:9092 --topic weather-data --from-beginning `
  --property print.key=true --property print.partition=true --property print.offset=true
#   Ctrl+C to stop

# B. Our Python validation consumer (summarises per city)
python scripts/consume_test.py            # reads everything, stops after 10s idle
python scripts/consume_test.py --follow   # keeps listening. Start the producer in another window.

# C. Topic details and message counts
docker exec weather-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic weather-data
docker exec weather-kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic weather-data

# D. Consumer groups and their lag
docker exec weather-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
docker exec weather-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 `
  --describe --group <group-name-from-list>
```

**Kafka UI** (http://localhost:8080):
* **Topics → weather-data → Overview**: partitions, message count per partition
* **Messages** tab: browse messages. Filter by key (e.g. `Delhi`) and click one to see its JSON
* **Consumers**: groups and lag per partition

## 6. Code
`scripts/consume_test.py`, the essential part:
```python
consumer = Consumer({
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    "group.id": f"weather-validator-{uuid.uuid4().hex[:6]}",   # new group each run
    "auto.offset.reset": "earliest",                           # new group -> start at the beginning
})
consumer.subscribe([topic])
while True:
    msg = consumer.poll(1.0)
    ...
    print(f"partition={msg.partition()} offset={msg.offset():<5} key={key:<10} {summary}")
```

## 7. Explanation: the Kafka concepts

| Concept | In this project |
|---|---|
| **Producer** | `ingestion/kafka_producer.py`. Writes events to a topic. It never knows or cares who reads them. |
| **Broker** | The Kafka server (`weather-kafka` container). Stores messages on disk. |
| **Topic** | `weather-data`: a named, append-only log of weather events. Like a table that only grows, where rows expire after 7 days (retention). |
| **Partition** | The topic is split into 3 logs. Each partition is ordered; there's **no** ordering *across* partitions. Keys decide the partition: `hash("Delhi") % 3`. |
| **Offset** | A message's position inside a partition (0, 1, 2 ...). `(partition, offset)` uniquely identifies a message. We store it in Snowflake for lineage. |
| **Consumer** | Reads messages in offset order and tracks where it is. |
| **Consumer group** | Consumers sharing a `group.id` **split** the partitions between them (3 partitions → at most 3 parallel consumers). Different groups each get **all** messages independently. That's why our validator never "steals" data from Spark. |
| **Lag** | Latest offset minus the group's committed offset: how far behind a consumer is. |

**Why 3 partitions?** It's enough to show key-based distribution and parallelism (up to 3 consumers in one group, or Spark reading 3 partitions in parallel) without pretending we need scale. 7 cities × 12 events/hour is tiny, and 1 partition would work too. You can add partitions later but never remove them. Adding them also changes which partition a key maps to, so pick a sensible number up front.

**Replication factor 1** because there's one broker. In production you'd use 3 brokers with RF=3 so a broker can die without data loss.

**Important for Phase 5: Spark does NOT use a consumer group to track progress.** Structured Streaming stores the offsets it has processed in its **checkpoint** folder. That's what gives it exactly-once bookkeeping. So Kafka UI won't show meaningful lag for Spark; use the Spark logs and Spark UI instead (Phase 11/monitoring).

## 8. Expected output
```
Consuming 'weather-data' as group 'weather-validator-3f9a1c'...
partition=0 offset=0     key=Mumbai     Mumbai: 29.4C Clouds @ 2026-09-28T08:50:00+00:00
partition=0 offset=1     key=Chennai    Chennai: 31.2C Haze @ 2026-09-28T08:50:00+00:00
partition=0 offset=2     key=Pune       Pune: 26.1C Rain @ 2026-09-28T08:50:00+00:00
partition=1 offset=0     key=Delhi      Delhi: 33.0C Haze @ 2026-09-28T08:50:00+00:00
...
Consumed 7 messages. Per key: {'Bengaluru': 1, 'Chennai': 1, 'Delhi': 1, 'Hyderabad': 1, 'Kolkata': 1, 'Mumbai': 1, 'Pune': 1}
```
Notice that each city always has the same partition.

## 9. Testing steps
1. Run `consume_test.py --follow` in one PowerShell window and `python -m ingestion.main --once` in another. 7 new lines appear within a second.
2. Run the producer twice. The per-key count is 2 for every city, and each city's partition stays the same.
3. Compare: `kafka-get-offsets` total = number of messages the validator consumed.
4. Open one message in Kafka UI and check that all fields from the event contract (`docs/00-architecture.md`) are present.

## 10. Common errors and fixes

| Problem | Fix |
|---|---|
| Console consumer shows nothing | Did you forget `--from-beginning`? Without it you only see *new* messages. |
| `consume_test.py` shows nothing | Is the producer publishing to the same `KAFKA_TOPIC`? Check `kafka-get-offsets`. |
| Consumer with a *fixed* group shows nothing on the 2nd run | Correct behaviour: that group already committed its offsets. Use a new group or reset: `kafka-consumer-groups.sh --group X --reset-offsets --to-earliest --topic weather-data --execute`. |
| Kafka UI "cluster offline" | Kafka UI must use `kafka:29092`, not `localhost:9092` (already set in compose). |

## 11. Verify before moving on
- [ ] You can read events with both the console consumer and `consume_test.py`
- [ ] Each city always goes to the same partition
- [ ] You can explain partition, offset, consumer group, and why Spark uses checkpoints instead of a group
