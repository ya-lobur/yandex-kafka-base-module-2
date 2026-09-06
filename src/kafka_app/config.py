"""Настройки Faust-приложения и Kafka-топиков."""

import os

APP_ID = "message-filter"

DEFAULT_BROKER_URL = "kafka://localhost:19092;localhost:29092;localhost:39092"
BROKER_URL = os.getenv("KAFKA_BROKERS", DEFAULT_BROKER_URL)
STORE_URL = os.getenv("FAUST_STORE", "memory://")

MESSAGES_TOPIC = "messages"
FILTERED_MESSAGES_TOPIC = "filtered_messages"
BLOCKED_USERS_TOPIC = "blocked_users"
BANNED_WORDS_TOPIC = "banned_words"

MESSAGE_PARTITIONS = 3
BANNED_WORDS_PARTITIONS = 1
REPLICATION_FACTOR = 3
