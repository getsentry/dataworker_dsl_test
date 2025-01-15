class AggregateChunks(CompositeStep):
    processor = ClickhouseProcessor

    def __init__(self, ctx, **kw):
        pipeline = Pipeline(name=kw["name"])

        reduce = Reduce(
            # group_by default = group by whatever the runtime
            # wants
            group_by="(org_id,trace_id)",
            inputs=kw['inputs'],
            create_acculumator=TraceBuffer,
            lambda acc, row: acc.add_span(row),
            # max_time=100,
            # max_size=100,
            should_flush=lambda client: client.should_flush(),
            ctx=pipeline,
            name='reduce',
        )

        Map(
            inputs=[reduce],
            lambda buffer: client.finalize_trace(),
            ctx=pipeline,
            name='map',
        )

        super().__init__(ctx, pipeline, **kw)

