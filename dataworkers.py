from library import *

class ClickhouseSink(CompositeStep):
    table: str
    processor = ClickhouseProcessor

    def __init__(self, ctx, **kw):
        pipeline = Pipeline(name=kw["name"])

        with ctx.in_same_worker():
            reduce = Reduce(
                inputs=kw['inputs'],
                create_acculumator=lambda: ClickhouseClient(),
                lambda client, row: client.write_row(row),
                # max_time=100,
                # max_size=100,
                should_flush=lambda client: client.should_flush(),
                ctx=pipeline,
                name='reduce',
            )

            Map(
                inputs=[reduce],
                lambda client: client.wait_for_ok(),
                ctx=pipeline,
                name='map',
            )

        super().__init__(ctx, pipeline, **kw)

class BigquerySink(CompositeStep):
    table: str
