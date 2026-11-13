CREATE TABLE IF NOT EXISTS events (
    device_id   text             NOT NULL,
    seq         bigint           NOT NULL,
    ts          timestamptz      NOT NULL,
    kind        text             NOT NULL
                CHECK (kind IN ('weight_g', 'water_ml', 'battery_pct', 'heartbeat')),
    value       double precision,
    received_at timestamptz      NOT NULL DEFAULT now(),
    PRIMARY KEY (device_id, seq)
);