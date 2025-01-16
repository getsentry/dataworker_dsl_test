from library import *

pipeline = Pipeline(name='transactions')

# # in app module:
# def get_killswitch_context(event):
    # return {
        # "project_id": event['project_id'],
    # }

source = KafkaSource(
    ctx=ctx,
    name='events-source',
    topic='ingest-events',
    ...
)

ks = Killswitch(
    ctx=pipeline,
    inputs=[source],
    name='kill-ingest',
    get_context='sentry.ingest.get_killswitch_context',
)

transform1 = EventEnricher(
    ctx=pipeline,
    inputs=[ks],
    rules=[
        'sentry.ingest.GetReleaseEnricher',
        'sentry.ingest.GetEnvironmentEnricher',
        'sentry.ingest.GetUserEnricher',
        'sentry.ingest.ExtractTagsEnricher',
        'sentry.ingest.GetAnotherReleaseEnricher',
    ]
)

# BEGIN PERSIST

# stream[T] -> stream[T]
nodestore = WriteNodestore(
    name='write-nodestore',
    ctx=pipeline,
    input=[transform1],
)

# stream[T] -> stream[T]
eventstream = WriteEventstream(
    name='write-eventstream',
    ctx=pipeline,
    input=[nodestore],
)

# END PERSIST

tsdb = WriteTSDB(
    inputs=[eventstream],
)

outcomes = WriteOutcomes(
    inputs=[eventstream],
)
