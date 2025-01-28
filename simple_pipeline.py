from mini_library import KafkaSource, Map, KafkaSink

source = KafkaSource(
    name='events-source',
    topic='ingest-events'
)

transform = Map(
    name='transform',
    func='sentry.ingest.something.process_message',
    inputs=[source],
)

sink = KafkaSink(
    name='events-sink',
    topic='snuba-events',
    inputs=[transform]
)

