#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════╗
║  PFA — Initialisation des Topics Kafka                       ║
║  Crée le topic traffic-events avec les bons paramètres       ║
╚══════════════════════════════════════════════════════════════╝
"""

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

TOPICS = [
    NewTopic(
        name="traffic-events",
        num_partitions=3,
        replication_factor=1,
        topic_configs={
            "retention.ms":          str(24 * 60 * 60 * 1000),  # 24 h
            "cleanup.policy":        "delete",
            "compression.type":      "lz4",
            "max.message.bytes":     str(1_000_000),
        },
    )
]


def setup_topics() -> None:
    admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    for topic in TOPICS:
        try:
            admin.create_topics([topic])
            print(f"[KAFKA] ✓ Topic créé : {topic.name}  "
                  f"({topic.num_partitions} partitions)")
        except TopicAlreadyExistsError:
            print(f"[KAFKA] ℹ Topic déjà existant : {topic.name}")
    admin.close()
    print("[KAFKA] ■ Configuration terminée.")


if __name__ == "__main__":
    setup_topics()
