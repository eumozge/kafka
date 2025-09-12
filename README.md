# Kafka Study Project

A simple study project demonstrating Kafka producers and consumers in Python with Avro and Schema Registry. It does not implement patterns such as inbox, outbox or external offset storage. Not using native AvroProducers/AvroConsumers for educational purposes only.

## Quick start
1. Install deps:
```bash
just install
```

2. Start Kafka stack (Zookeeper, Kafka, Schema Registry, Kafka UI), uses dev.storages.yaml and .env:
```bash
just storages
```

3. Run a producer, for example produce 100 messages with 100ms delay between messages:
```bash
just producers start --message-count 100 --message-delay 100
```

4. Run a consumer (in another terminal), for example consume with group "default":
```bash
just consumers start --group default --message-delay 100
```

5. Stop the stack:
```bash
just storages-down
```

## Environment
Create `.env` from `.env.example` in the project root (used by Docker compose and the app):

## What it does

- Producer generates random events and publishes to Kafka.
- Avro schemas are auto-registered on startup from via Schema Registry.
- Consumer subscribes to the topic, deserializes events via the registered schema.
