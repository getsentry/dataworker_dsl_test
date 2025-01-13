from library import *

class ClickhouseSink(CompositeStep):
    table: str
    processor = ClickhouseProcessor

    def __init__(self, ctx, **kw):
        pipeline = Pipeline(name=kw["name"])

        reduce = Reduce(
            inputs=kw['inputs'],
            lambda acc, x: acc.add(x),
            ctx=pipeline,
            name='reduce',
        )

        Map(
            inputs=[reduce],
            lambda x: print("waiting for clickhouse"),
            ctx=pipeline,
            name='map',
        )

        super().__init__(ctx, pipeline, **kw)

class BigquerySink(CompositeStep):
    table: str
