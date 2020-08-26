-- -- 开启 mini-batch
-- SET table.exec.mini-batch.enabled=true;
-- -- mini-batch的时间间隔，即作业需要额外忍受的延迟
-- SET table.exec.mini-batch.allow-latency=1s;
-- -- 一个 mini-batch 中允许最多缓存的数据
-- SET table.exec.mini-batch.size=1000;
-- -- 开启 local-global 优化
-- SET table.optimizer.agg-phase-strategy=TWO_PHASE;
--
-- -- 开启 distinct agg 切分
-- SET table.optimizer.distinct-agg.split.enabled=true;

-- source
CREATE TABLE user_log (
    user_id BIGINT,
    item_id BIGINT,
    category_id INT,
    behavior VARCHAR,
    ts BIGINT
) WITH (
    'connector.type' = 'kafka',
    'connector.version' = 'universal',
    'connector.topic' = 'flink_table2',
    'connector.startup-mode' = 'earliest-offset',
    'connector.properties.0.key' = 'zookeeper.connect',
    'connector.properties.0.value' = '192.168.224.129:2181',
    'connector.properties.1.key' = 'bootstrap.servers',
    'connector.properties.1.value' = '192.168.224.129:9092',
    'update-mode' = 'append',
    'format.type' = 'json',
    'format.derive-schema' = 'true'
);
