from dataclasses import dataclass

Message = int

class Processor:
    def process_message(self, input: Message) -> list[Message]:
        ...

@dataclass
class _ProcessingStage:

class ProcessingStage(_ProcessingStage):
    name: str

    def __init__(self, ctx: Pipeline, **kw):
        super().__init__(**kw)
        ctx.register_stage(self)


class Source(ProcessingStage):
    pass

class Transform(ProcessingStage):
    inputs: list[ProcessingStage]

class Map(Transform):
    func: str

class KafkaSource(Source):
    topic: str

class ClickhouseSink(ProcessingStage):
    table: str

class BigquerySink(ProcessingStage):
    table: str

class Pipeline:
    def __init__(self, name):
        self.stages = []
        self.name = name

    def register_stage(self, stage: ProcessingStage):
        self.stages.append(stage)

    def start(self):
        pass
