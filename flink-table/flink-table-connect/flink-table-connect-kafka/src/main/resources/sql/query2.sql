-- sink
CREATE TABLE pvuv_sink (
    dt VARCHAR,
    pv BIGINT,
    uv BIGINT
) WITH (
    'connector.type' = 'jdbc',
    'connector.url' = 'jdbc:mysql://localhost:3306/boot?useUnicode=true&characterEncoding=utf-8&serverTimezone=Asia/Shanghai',
    'connector.table' = 'pvuv_sink',
    'connector.username' = 'root',
    'connector.password' = '1121',
    'connector.write.flush.max-rows' = '1'
);
