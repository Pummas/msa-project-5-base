CREATE TABLE shipment_events (
    event_id BIGSERIAL PRIMARY KEY,
    shipment_id BIGINT NOT NULL,
    event_type TEXT NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    location TEXT
);

INSERT INTO shipment_events (shipment_id, event_type, event_time, location) VALUES
    (10001, 'picked_up', '2026-11-13T08:15:00Z', 'Moscow'),
    (10001, 'in_transit', '2026-11-13T12:30:00Z', 'Tver'),
    (10002, 'delivered', '2026-11-13T16:45:00Z', 'Saint Petersburg');
