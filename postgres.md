# Technical Document: Confluent Kafka with Debezium PostgreSQL CDC

## 1. Purpose

This document outlines how to implement a Confluent Kafka pipeline using PostgreSQL as a CDC source through the Debezium PostgreSQL connector. It also explains how PostgreSQL must be configured so Debezium can read from the Write-Ahead Log, or WAL, using logical replication.

Important clarification: the Debezium PostgreSQL connector is a **source connector**. For writing Kafka topic data back into a database, use a **sink connector**, typically the Debezium JDBC sink connector or Confluent JDBC Sink connector. Debezium’s PostgreSQL source connector snapshots existing data and then streams row-level changes into Kafka topics. ([Confluent Docs][1])

---

## 2. High-Level Architecture

```text
PostgreSQL Source DB
        |
        | WAL / Logical Decoding
        |
Debezium PostgreSQL Source Connector
        |
        | Kafka Connect
        v
Confluent Kafka Topics
        |
        | Optional transforms / schema registry
        v
JDBC Sink Connector
        |
        v
Target PostgreSQL / SQL Database
```

Core components:

| Component                            | Purpose                                                                   |
| ------------------------------------ | ------------------------------------------------------------------------- |
| PostgreSQL WAL                       | Durable transaction log used by Debezium to capture committed row changes |
| Logical Replication Slot             | Retains WAL segments until Debezium has consumed them                     |
| Publication                          | Defines which tables publish changes                                      |
| Debezium PostgreSQL Source Connector | Reads changes from WAL and writes change events to Kafka                  |
| Kafka Topics                         | Store CDC events, usually one topic per captured table                    |
| JDBC / Debezium JDBC Sink Connector  | Consumes Kafka topics and writes to the destination database              |

---

## 3. PostgreSQL WAL and Logical Replication Overview

PostgreSQL records database changes in the WAL before applying them to data files. Debezium uses PostgreSQL logical decoding to convert WAL changes into row-level change events. PostgreSQL logical decoding uses replication slots, and those slots retain WAL segments required by Debezium even if the connector is temporarily offline. This is useful for reliability, but it also means replication slot lag must be monitored to avoid excessive disk usage. ([Debezium][2])

Debezium commonly uses the `pgoutput` logical decoding plugin, which is included with PostgreSQL 10+ and maintained by the PostgreSQL community. ([Debezium][3])

---

## 4. PostgreSQL Database Requirements

### 4.1 Enable Logical WAL

Update `postgresql.conf`:

```conf
wal_level = logical
max_replication_slots = 10
max_wal_senders = 10
```

Minimum required settings are typically:

```conf
wal_level = logical
max_replication_slots = 1
max_wal_senders = 1
```

For production, allocate more than one slot and sender to allow monitoring, maintenance, and future connector expansion. PostgreSQL requires `wal_level=logical` for logical decoding and replication slots. ([Red Hat Documentation][4])

Restart PostgreSQL after changing `wal_level`.

---

### 4.2 Create a Replication User

Create a dedicated Debezium user:

```sql
CREATE ROLE debezium_user WITH LOGIN PASSWORD 'REPLACE_WITH_STRONG_PASSWORD';

ALTER ROLE debezium_user WITH REPLICATION;
```

Grant database connection access:

```sql
GRANT CONNECT ON DATABASE appdb TO debezium_user;
```

Grant schema and table access:

```sql
GRANT USAGE ON SCHEMA public TO debezium_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT SELECT ON TABLES TO debezium_user;
```

---

### 4.3 Configure `pg_hba.conf`

Allow the Kafka Connect worker or Debezium host to connect to PostgreSQL.

Example:

```conf
host    appdb        debezium_user     10.10.0.0/16       scram-sha-256
host    replication  debezium_user     10.10.0.0/16       scram-sha-256
```

Reload PostgreSQL:

```bash
SELECT pg_reload_conf();
```

---

### 4.4 Ensure Tables Have Primary Keys

Debezium works best when captured tables have primary keys. The primary key becomes the Kafka record key and is important for downstream upserts and deletes.

Example:

```sql
ALTER TABLE public.customer
ADD CONSTRAINT customer_pk PRIMARY KEY (id);
```

Tables without primary keys can still be captured, but update/delete semantics become harder to handle downstream.

---

### 4.5 Create a Publication

For selected tables:

```sql
CREATE PUBLICATION debezium_publication
FOR TABLE public.customer, public.orders, public.invoice;
```

For all tables:

```sql
CREATE PUBLICATION debezium_publication FOR ALL TABLES;
```

For controlled production systems, prefer explicit table lists.

---

### 4.6 Optional: Create the Replication Slot Manually

Debezium can create the slot automatically, but manual creation is useful when access is tightly controlled.

```sql
SELECT pg_create_logical_replication_slot('debezium_slot', 'pgoutput');
```

A Debezium source should have its own dedicated replication slot. ([Neon][5])

---

## 5. Debezium PostgreSQL Source Connector Configuration

Example connector configuration:

```json
{
  "name": "postgres-source-appdb",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "tasks.max": "1",

    "database.hostname": "postgres-source.internal",
    "database.port": "5432",
    "database.user": "debezium_user",
    "database.password": "REPLACE_WITH_SECRET",
    "database.dbname": "appdb",

    "topic.prefix": "appdb",
    "plugin.name": "pgoutput",

    "slot.name": "debezium_slot",
    "publication.name": "debezium_publication",

    "schema.include.list": "public",
    "table.include.list": "public.customer,public.orders,public.invoice",

    "snapshot.mode": "initial",

    "tombstones.on.delete": "true",

    "decimal.handling.mode": "precise",
    "time.precision.mode": "adaptive_time_microseconds",

    "heartbeat.interval.ms": "10000"
  }
}
```

Expected Kafka topic naming:

```text
appdb.public.customer
appdb.public.orders
appdb.public.invoice
```

---

## 6. Snapshot Behavior

The source connector normally performs an initial snapshot before streaming WAL changes.

Common `snapshot.mode` values:

| Mode                                 | Use Case                                                                           |
| ------------------------------------ | ---------------------------------------------------------------------------------- |
| `initial`                            | First-time load plus ongoing CDC                                                   |
| `never`                              | Only stream new changes from WAL                                                   |
| `schema_only` / schema-focused modes | Capture schema metadata without loading table data, depending on connector version |
| `when_needed`                        | Snapshot when offsets are missing or invalid                                       |

For production migrations, use `initial` unless the target is already loaded and reconciled.

---

## 7. Sink Connector Options

### Option A: Debezium JDBC Sink Connector

The Debezium JDBC connector is a Kafka Connect sink connector that consumes Kafka topics and writes records to relational databases using JDBC. It supports databases such as PostgreSQL, MySQL, Oracle, SQL Server, and Db2, and supports idempotent writes using upsert semantics. ([Debezium][6])

Example:

```json
{
  "name": "postgres-target-sink",
  "config": {
    "connector.class": "io.debezium.connector.jdbc.JdbcSinkConnector",
    "tasks.max": "2",

    "connection.url": "jdbc:postgresql://postgres-target.internal:5432/appdb_target",
    "connection.username": "sink_user",
    "connection.password": "REPLACE_WITH_SECRET",

    "topics": "appdb.public.customer,appdb.public.orders,appdb.public.invoice",

    "insert.mode": "upsert",
    "delete.enabled": "true",
    "primary.key.mode": "record_key",

    "schema.evolution": "basic"
  }
}
```

### Option B: Confluent JDBC Sink Connector

The Confluent JDBC Sink connector supports `insert`, `upsert`, and `update` write modes. ([Confluent Docs][7]) Deletes can be enabled with `delete.enabled=true`, but Confluent documents that this requires `pk.mode=record_key` because deletes need the record key to identify the row. ([Confluent Docs][8])

Example:

```json
{
  "name": "jdbc-sink-postgres-target",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "tasks.max": "2",

    "connection.url": "jdbc:postgresql://postgres-target.internal:5432/appdb_target",
    "connection.user": "sink_user",
    "connection.password": "REPLACE_WITH_SECRET",

    "topics": "appdb.public.customer,appdb.public.orders,appdb.public.invoice",

    "insert.mode": "upsert",
    "pk.mode": "record_key",
    "delete.enabled": "true",

    "auto.create": "false",
    "auto.evolve": "false"
  }
}
```

---

## 8. Change Event Handling

Debezium emits structured CDC events. Typical operations:

| Operation | Meaning         |
| --------- | --------------- |
| `c`       | Create / insert |
| `u`       | Update          |
| `d`       | Delete          |
| `r`       | Snapshot read   |

A delete usually emits a delete event followed by a tombstone event when `tombstones.on.delete=true`. This is useful for compacted Kafka topics.

---

## 9. Kafka Topic Design

Recommended topic settings for CDC topics:

```bash
cleanup.policy=compact
min.cleanable.dirty.ratio=0.1
retention.ms=604800000
```

For audit/event-history use cases, consider:

```bash
cleanup.policy=delete
```

For current-state replication use cases, compacted topics are usually preferred.

---

## 10. Operational Monitoring

Monitor PostgreSQL replication slots:

```sql
SELECT
    slot_name,
    plugin,
    slot_type,
    active,
    restart_lsn,
    confirmed_flush_lsn,
    pg_size_pretty(
        pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)
    ) AS retained_wal
FROM pg_replication_slots;
```

Monitor active replication:

```sql
SELECT
    pid,
    usename,
    application_name,
    client_addr,
    state,
    sent_lsn,
    write_lsn,
    flush_lsn,
    replay_lsn
FROM pg_stat_replication;
```

Key risks:

| Risk                 | Cause                                   | Mitigation                                               |
| -------------------- | --------------------------------------- | -------------------------------------------------------- |
| WAL disk growth      | Connector offline or slot not advancing | Alert on retained WAL size                               |
| Connector lag        | Kafka Connect slow or unavailable       | Monitor Connect task status and consumer lag             |
| Schema drift         | Source table changes                    | Use Schema Registry and schema governance                |
| Missing deletes      | Sink not configured for delete handling | Use record keys and enable delete support                |
| Snapshot load impact | Large tables                            | Use off-peak snapshots and incremental snapshot strategy |

---

## 11. Security Requirements

Recommended controls:

| Area            | Recommendation                                                            |
| --------------- | ------------------------------------------------------------------------- |
| PostgreSQL user | Dedicated least-privilege Debezium user                                   |
| Network         | Restrict PostgreSQL access to Kafka Connect workers                       |
| TLS             | Use SSL/TLS for PostgreSQL and Kafka connections                          |
| Secrets         | Store connector passwords in Confluent secrets or external secret manager |
| Kafka ACLs      | Limit connector access to required topics only                            |
| Audit           | Log connector configuration changes                                       |

---

## 12. Validation Checklist

### PostgreSQL

```sql
SHOW wal_level;
SHOW max_replication_slots;
SHOW max_wal_senders;
```

Expected:

```text
wal_level = logical
```

Check publication:

```sql
SELECT * FROM pg_publication;
SELECT * FROM pg_publication_tables;
```

Check slot:

```sql
SELECT * FROM pg_replication_slots;
```

### Kafka Connect

Check connector status:

```bash
curl http://connect-host:8083/connectors/postgres-source-appdb/status
```

### Kafka Topic Validation

Consume records:

```bash
kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic appdb.public.customer \
  --from-beginning \
  --property print.key=true
```

### End-to-End Test

Insert:

```sql
INSERT INTO public.customer (id, name, email)
VALUES (1, 'Test Customer', 'test@example.com');
```

Update:

```sql
UPDATE public.customer
SET email = 'updated@example.com'
WHERE id = 1;
```

Delete:

```sql
DELETE FROM public.customer
WHERE id = 1;
```

Confirm:

1. Events appear in Kafka.
2. Sink connector consumes the events.
3. Target database reflects insert, update, and delete behavior.
4. Replication slot advances.

---

## 13. Recommended Production Pattern

Use this pattern for a clean production implementation:

```text
PostgreSQL Source
  - wal_level=logical
  - dedicated replication user
  - explicit publication
  - dedicated replication slot
  - primary keys on all replicated tables

Kafka Connect
  - Debezium PostgreSQL source connector
  - Schema Registry enabled
  - DLQ configured
  - connector secrets externalized

Kafka
  - one topic per source table
  - compaction enabled for stateful replication topics
  - ACLs restricted by connector role

Target Database
  - JDBC sink connector
  - upsert mode
  - delete handling enabled
  - matching primary keys
```

---

## 14. Key Takeaways

The Debezium PostgreSQL connector reads committed row-level changes from PostgreSQL WAL through logical decoding. PostgreSQL must be configured with `wal_level=logical`, sufficient replication slots, sufficient WAL senders, a replication-capable user, and a publication defining the tables to stream. Debezium acts as the source connector into Kafka. A separate sink connector, such as Debezium JDBC Sink or Confluent JDBC Sink, should be used to write Kafka CDC events into a target database.

[1]: https://docs.confluent.io/kafka-connectors/debezium-postgres-source/current/overview.html?utm_source=chatgpt.com "Debezium PostgreSQL Source Connector for Confluent ..."
[2]: https://debezium.io/documentation/reference/1.9/connectors/postgresql.html?utm_source=chatgpt.com "Debezium connector for PostgreSQL"
[3]: https://debezium.io/documentation/reference/stable/connectors/postgresql.html?utm_source=chatgpt.com "Debezium connector for PostgreSQL"
[4]: https://docs.redhat.com/en/documentation/red_hat_integration/2021.q3/html/debezium_user_guide/debezium-connector-for-postgresql?utm_source=chatgpt.com "Chapter 7. Debezium connector for PostgreSQL"
[5]: https://neon.com/docs/guides/logical-replication-kafka-confluent?utm_source=chatgpt.com "Replicate data with Kafka (Confluent) and Debezium"
[6]: https://debezium.io/documentation/reference/stable/connectors/jdbc.html?utm_source=chatgpt.com "Debezium connector for JDBC"
[7]: https://docs.confluent.io/kafka-connectors/jdbc/current/sink-connector/sink_config_options.html?utm_source=chatgpt.com "Configuration Reference for JDBC Sink Connector ..."
[8]: https://docs.confluent.io/kafka-connectors/jdbc/current/sink-connector/overview.html?utm_source=chatgpt.com "JDBC Sink Connector for Confluent Platform"
