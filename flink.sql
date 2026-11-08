SET 'table.local-time-zone' = 'UTC';
SET 'parallelism.default' = '1';
SET 'execution.checkpointing.interval' = '30s';

CREATE TABLE quotes_raw (
  symbol STRING,
  price DOUBLE,
  event_time TIMESTAMP_LTZ(3),
  kafka_partition INT METADATA FROM 'partition' VIRTUAL,
  kafka_offset BIGINT METADATA FROM 'offset' VIRTUAL,
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'quotes.raw',
  'properties.bootstrap.servers' = 'redpanda:9092',
  'properties.group.id' = 'flink-cotations-minute',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601'
);

CREATE TABLE quotes_minute (
  symbol STRING,
  window_start TIMESTAMP(3),
  window_end TIMESTAMP(3),
  avg_price DOUBLE,
  quote_count BIGINT,
  open_price DOUBLE,
  high_price DOUBLE,
  low_price DOUBLE,
  close_price DOUBLE
) WITH (
  'connector' = 'kafka',
  'topic' = 'quotes.minute',
  'properties.bootstrap.servers' = 'redpanda:9092',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601'
);

-- The prefix sorts by UTC event time, then by Kafka partition and offset.
CREATE VIEW ordered_quotes AS
SELECT *, CONCAT(DATE_FORMAT(event_time, 'yyyyMMddHHmmssSSS'), '|',
                 LPAD(CAST(kafka_partition AS STRING), 10, '0'), '|',
                 LPAD(CAST(kafka_offset AS STRING), 19, '0'), '|',
                 CAST(price AS STRING)) AS ordered_price
FROM quotes_raw;

INSERT INTO quotes_minute
SELECT symbol, window_start, window_end, AVG(price), COUNT(*),
       CAST(SPLIT_INDEX(MIN(ordered_price), '|', 3) AS DOUBLE),
       MAX(price), MIN(price),
       CAST(SPLIT_INDEX(MAX(ordered_price), '|', 3) AS DOUBLE)
FROM TABLE(TUMBLE(TABLE ordered_quotes, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))
GROUP BY symbol, window_start, window_end;
