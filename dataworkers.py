from library import *

class ClickhouseSink(CompositeStep):
    table: str
    processor = ClickhouseProcessor

    def __init__(self, ctx, **kw):
        pipeline = Pipeline(name=kw["name"])

        within_worker_start = WithinWorker()

        reduce = Reduce(
            inputs=kw['inputs'],
            create_acculumator=lambda: ClickhouseClient(),
            lambda client, row: client.write_row(row),
            ctx=pipeline,
            name='reduce',
        )

        Map(
            inputs=[reduce],
            lambda client: client.wait_for_ok(),
            ctx=pipeline,
            name='map',
        )

        within_worker_end = AcrossWorkers()

        super().__init__(ctx, pipeline, **kw)

class BigquerySink(CompositeStep):
    table: str
