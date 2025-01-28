from collections import defaultdict, deque


# The following implementation uses 
# the name of the step as the node representation
# Use a hashable Node representation

class PipelineGraph:

    def __init__(self, pipeline_steps):
        self.logical_graph = defaultdict(set)
        self.zero_indegree_queue = deque([])
        self.indegrees = defaultdict(int)

        self.pipeline_steps = pipeline_steps

        self._construct()

    def _construct(self):

        for step_name in self.pipeline_steps:

            val = self.pipeline_steps[step_name]

            if not hasattr(val, "inputs"):
                self.zero_indegree_queue.append(step_name)

            else:
                inputs = val.inputs
                for input in inputs:
                    self.logical_graph[input.name].add(step_name)
                    self.indegrees[step_name] += 1     

    # clear the queue and return, populate the queue with next steps
    def next_steps(self):
        next_steps = []

        source_count = len(self.zero_indegree_queue)

        for _ in range(source_count):
            step = self.zero_indegree_queue.popleft()
            next_steps.append(step)

            for neighbor in self.logical_graph[step]:
                self.indegrees[neighbor] -= 1
                if self.indegrees[neighbor] == 0:
                    self.zero_indegree_queue.append(neighbor)

        return next_steps
    

    # some function to refresh graph state


# One could call next_step() on the graph and create another Pipeline 
# representation, which can have its own Visitor class
