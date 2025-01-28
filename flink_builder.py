from typing import Optional
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.window import SlidingProcessingTimeWindows
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import Time

class FlinkStreamBuilder:

    def __init__(self, pipeline_steps, graph, config: Optional[dict]):
        self.pipeline_map = pipeline_steps
        self.graph = graph

        self.env = StreamExecutionEnvironment.get_execution_environment()
        # process the config to set up env

        # initialize the Flink stream using the sources
        self.stream = self._set_sources()


    def _set_sources(self):

        sources = self.graph.get_next_steps()
        steps = [self.pipeline_map[source] for source in sources]

        kafka_consumer = FlinkKafkaConsumer(
            topics=[step.topic for step in steps],
            deserialization_schema=SimpleStringSchema(),
            properties={
                "bootstrap.servers": 123,
                "group.id": "riyas-random-consumer"
            }
        )
        
        # define the source
        kafka_consumer.set_start_from_earliest()
        stream = self.env.add_source(kafka_consumer)

        return stream

    def build_stream(self):

        stream = self._set_sources()

        while True:

            steps = self.graph.get_next_steps()

            # we're done traversing the graph
            if not steps:
                break

            # process the step to map it to a Flink operator 
            # this needs to be some form of parallel execution
            # serial for now
            for step in steps:

                # some mapping from our API to Flink API
                # below assumes they are the same
                physical_step = self.pipeline_map[step]
                flink_step_name = physical_step.__class__.__name__.lower()

                # dynamically call the flink step
                # right now, this assumes a Map step 
                # it needs to be able to handle any kind of step
                flink_step = getattr(stream, flink_step_name)
                stream = flink_step(physical_step.func)


    def submit_stream(self):
        self.env.execute()
