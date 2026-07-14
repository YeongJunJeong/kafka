## What is Kafka

In the old system:
A -> B
A -> C
A -> D
Had to hand off info to each one individually.

With Kafka:
A -> central system
Drop it off there, and B, C, D each pick it up whenever they're free.

This is the key idea.

- Kafka = distributed event streaming platform. Built at LinkedIn, later open-sourced under Apache.
- Post office analogy: Producer drops a message off at Kafka (the post office); Consumers pick it up whenever they're ready. Producer doesn't need to know who's listening.
- Topic: named channel where messages are stored. Producer writes to it, Consumer reads from it.
- Producer -> Kafka (stored on disk) <- Consumer (pull-based, Kafka doesn't push to consumers)
- Reading a message doesn't delete it. Each Consumer tracks its own offset -> a new consumer can re-read from the start with --from-beginning.
- Storage: append-only log files (never edited in place, only appended -> fast writes). Example path: /tmp/kraft-combined-logs/<topic>-0
- Retention deletes old messages after a set time (default ~7 days, KAFKA_LOG_RETENTION_HOURS), regardless of whether they've been read.
- Kafka is just a temporary relay for data in transit — no query/search capability. The DB is where data actually lives long-term.
  - Order System -> Kafka -> Inventory Consumer -> Inventory DB (same pattern for Payment, Logging, etc.)

**Docker hands-on**
- docker pull apache/kafka:latest
- docker run -d -p 9092:9092 --name Kafka apache/kafka:latest (KRaft mode, no Zookeeper needed)
- docker exec -it Kafka bash -> cd /opt/kafka/bin
- Create topic: ./kafka-topics.sh --create --topic my-topic --bootstrap-server localhost:9092
- List topics: ./kafka-topics.sh --list --bootstrap-server localhost:9092
- Start consumer: ./kafka-console-consumer.sh --topic my-topic --bootstrap-server localhost:9092
- Start producer: ./kafka-console-producer.sh --topic my-topic --bootstrap-server localhost:9092
- Messages typed in producer showed up instantly in consumer -> confirms pull-based, decoupled model.
- docker-compose.yml makes it repeatable (services.kafka.image: apache/kafka:latest, ports 9092:9092)
  - up -d / stop / start / down. down removes data if no volume configured.
  - compose commands must be run from the folder containing docker-compose.yml.

**WSL2 / Docker Desktop**
- Docker Desktop runs on top of WSL2.
- "wsl is not installed" error -> run `wsl --install` in admin PowerShell, then reboot.
- `wsl --list --verbose` shows distro states (docker-desktop, docker-desktop-data).
- VmmemWSL can keep holding memory even after distros stop -> `wsl --shutdown` to fully stop, or cap memory via .wslconfig.

**Local setup vs. production**
- Local: single broker, no replication, no persistent volume (data lost), no security/monitoring.
- Production: cluster of 3+ brokers, replication.factor copies data -> if one broker dies, a replica broker is promoted to leader.
- Brokers spread across data centers/availability zones to avoid a single point of failure.
- Critical systems may use multi-region replication (MirrorMaker) — costly, only when downtime is unacceptable.
- Persistent storage, monitoring (Prometheus/Grafana), security (TLS/ACL), per-topic retention tuning.
- Usually run via Kubernetes + an operator (Strimzi) or managed service (AWS MSK, Confluent Cloud).
- Key idea: nothing is 100% failure-proof — production Kafka just reduces the probability/blast radius of failure to an acceptable level.
