class AggregateChunks(CompositeStep):
    processor = ClickhouseProcessor

    def __init__(self, ctx, **kw):
        pipeline = Pipeline(name=kw["name"])

        # input: span
        # {"org_id": 1, "span_id": 1, "trace_id": "abc"}
        # {"org_id": 1, "span_id": 2, "trace_id": "def"}
        # {"org_id": 1, "span_id": 3, "trace_id": "abc"}
        # output: tracebuffer for specific (org_id,trace_id)
        # TraceBuffer([1, 3])
        # TraceBuffer([2])
        reduce = Reduce(
            storage_type='redis',
            # group_by default = group by whatever the runtime
            # wants
            group_by="trace_id",
            inputs=kw['inputs'],
            create_acculumator=TraceBuffer,
            lambda acc, row: acc.add_message(row),
            # max_time=100,
            # max_size=100,
            should_flush=lambda acc: acc.should_flush() ,
            ctx=pipeline,
            name='reduce',
        )

        Map(
            concurrency=1,
            inputs=[reduce],
            lambda buffer: client.process_messages(),
            ctx=pipeline,
            name='map',
        )

        super().__init__(ctx, pipeline, **kw)

