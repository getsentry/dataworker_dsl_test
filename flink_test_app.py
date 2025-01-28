import simple_pipeline
from graph_builder import PipelineGraph
from flink_builder import FlinkStreamBuilder

# generate stream graph
pipeline_steps = {value.name: value for step, value in simple_pipeline.__dict__.items() if not step.startswith('_') and not callable(value)}
pipeline_graph = PipelineGraph(pipeline_steps)
config = {}

# create Flink lifecycle
stream_builder = FlinkStreamBuilder(pipeline_steps, pipeline_graph, config)
stream_builder.build_stream()

# execute Flink app
stream_builder.submit_stream()
