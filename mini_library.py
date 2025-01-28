from dataclasses import dataclass
from typing import List

Message = int

class Processor:
    def process_message(self, input: Message) -> List[Message]:
        ...

@dataclass
class _ProcessingStage:
    pass

@dataclass
class ProcessingStage(_ProcessingStage):
    name: str

@dataclass
class Source(ProcessingStage):
    ...

@dataclass
class Sink(ProcessingStage):
    inputs: List[ProcessingStage]

@dataclass
class Transform(ProcessingStage):
    inputs: List[ProcessingStage]

class MapProcessor(Processor):
    ...

class ReduceProcessor(Processor):
    ...

@dataclass
class Map(Transform):
    func: str

class Reduce(Transform):
    func: str
    processor = ReduceProcessor

@dataclass
class KafkaSource(Source):
    topic: str

@dataclass
class KafkaSink(Sink):
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
