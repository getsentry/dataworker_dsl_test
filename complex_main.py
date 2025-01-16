from library import *
from complex_dataworkers import *

ctx = Pipeline(name='mypipeline')

source = KafkaSource(
    ctx=ctx,
    name='events-source',
    topic='ingest-topic',
    ...
)

filter = Filter(
    ctx=ctx,
    func='sentry.ingest.something.filter_message',
    inputs=[source],
)

transform = Map(
    ctx=ctx,
    func='sentry.ingest.something.process_message',
    inputs=[filter],
)

another_source = KafkaSource(
    name='another-ingest-topic',
    topic='ingest-another',
    ...
)

# synchronized consumer
# (stream1[A], stream2[B]) -> stream3[A]
synchronizer = Synchronizer(
    name='synchronizer',
    inputs=[another_source],
    synchronize_with=[transform],
    input_key=['foo'],
    synchronization_key=['another_foo']
    func=p95,
    window_size=10,
)

# alternative interpretations of "synchronizer":

# (stream1[A], stream2[B]) -> stream3[(A, B)]
# TODO: we want to handle cases where elements cannot be matched up
# zipper = Zip(
    # zip_inputs=[transform, another_source],
    # zip_keys=['foo', 'another_foo'],
# )

# # (stream1[A], stream2[B]) -> (stream3[A], stream4[B])
# # XXX: unclear if we need this over zip
# # TODO: we want to handle cases where elements cannot be matched up
# fences = Fence(
    # sync_inputs=[transform, another_source],
    # sync_keys=['foo', 'another_foo'],
# )
    
# (spans) -> traces
aggregate_chunks = AggregateChunks(
    ctx=ctx,
    name='aggregate-chunks',
    inputs=[synchronizer],
)
