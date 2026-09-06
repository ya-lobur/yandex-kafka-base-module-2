-- Аналитика входящего потока messages для второго задания практической работы.
-- Запросы выполняются после запуска ksqldb-server командой make ksql-apply.

SET 'auto.offset.reset' = 'earliest';

CREATE STREAM messages_stream (
    user_id STRING,
    recipient_id STRING,
    message STRING,
    timestamp BIGINT
) WITH (
    KAFKA_TOPIC = 'messages',
    KEY_FORMAT = 'KAFKA',
    VALUE_FORMAT = 'JSON',
    TIMESTAMP = 'timestamp'
);

CREATE TABLE total_messages AS
SELECT 'all' AS scope,
       COUNT(*) AS message_count
FROM messages_stream
GROUP BY 'all'
EMIT CHANGES;

CREATE TABLE unique_recipients AS
SELECT 'all' AS scope,
       COUNT_DISTINCT(recipient_id) AS recipient_count
FROM messages_stream
GROUP BY 'all'
EMIT CHANGES;

CREATE TABLE user_statistics
WITH (
    KAFKA_TOPIC = 'user_statistics',
    KEY_FORMAT = 'KAFKA',
    VALUE_FORMAT = 'JSON'
) AS
SELECT user_id,
       COUNT(*) AS sent_messages,
       COUNT_DISTINCT(recipient_id) AS unique_recipients
FROM messages_stream
GROUP BY user_id
EMIT CHANGES;
