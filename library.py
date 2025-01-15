from dataclasses import dataclass

Message = int

class Processor:
    def process_message(self, input: Message) -> list[Message]:
        ...

@dataclass
class _ProcessingStage:
    pass

class ProcessingStage(_ProcessingStage):
    name: str

    def __init__(self, ctx: Pipeline, **kw):
        super().__init__(**kw)
        ctx.register_stage(self)


class CompositeStage(ProcessingStage):
    def __init__(self, ctx: Pipeline, pipeline: Pipeline, **kw):
        super().__init__(**kw)

        for step in pipeline.iter_steps():
            ctx.register_stage(step.replace(name=f"{pipeline.name}-{step.name}"))

class Source(ProcessingStage):
    pass

class Transform(ProcessingStage):
    inputs: list[ProcessingStage]

class MapProcessor(Processor):
    def __init__(self, config: Map):
        self.config = config
        module_name, func_name = config.func.rsplit('.', 1)
        self.func = getattr(__import__(module_name), func_name)

    def process_message(self, msg):
        new_value = self.func(msg)
        return [new_value]

class ReduceProcessor(Processor):
    ...

class Map(Transform):
    func: str
    processor = MapProcessor

class Reduce(Transform):
    func: str
    processor = ReduceProcessor

class KafkaSource(Source):
    topic: str

# TODO: group by!! we want multiple aggregates, one reduce state
# per trace

class Pipeline:
    def __init__(self, name):
        self.stages = []
        self.name = name

    def register_stage(self, stage: ProcessingStage):
        self.stages.append(stage)

    def start(self):
        pass
