CREATE TABLE inbox_messages (
    consumer_name TEXT NOT NULL,
    message_id TEXT NOT NULL,
    hash TEXT NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,

    PRIMARY KEY (consumer_name, message_id)
);

COMMENT ON TABLE inbox_messages IS 'Durable dedup record for SQS message consumption. The (consumerName, messageId) primary key is the deduplication key; completed_at is set only after the domain transaction it belongs to has committed.';
