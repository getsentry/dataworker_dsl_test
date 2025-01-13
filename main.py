from library import *

ctx = Pipeline(name='mypipeline')

source = KafkaSource(
    ctx=ctx,
    name='events-source',
    topic='ingest-events',
    ...
)

transform = Map(
    ctx=ctx,
    func='sentry.ingest.something.process_message',
    inputs=[source],
)

ch_sink = ClickhouseSink(
    ctx=ctx,
    name='errors-clickhouse-writer',
    inputs=[transform],
    table='errors',
)

bigquery_sink = BigquerySink(
    name='errors-bq',
    ctx=ctx,
    inputs=[transform],
    table='sbc_errors',
)

ctx.start()
