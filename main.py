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
    # want_gpu=True,
)

# TODO: we should probably have output here and not call that
# Sink
ClickhouseSink(
    ctx=ctx,
    name='errors-clickhouse-writer',
    inputs=[transform],
    table='errors',
)

BigquerySink(
    ctx=ctx,
    name='errors-bq',
    inputs=[transform],
    table='sbc_errors',
)

ctx.start()
