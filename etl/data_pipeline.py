from etl.pipeline_step import PipelineStep
from typing import Optional, Union
from collections.abc import Collection


class DataPipeline():
    def __init__(self, *steps: Optional[Union[Collection[PipelineStep], PipelineStep]], data=[]):
        if len(steps) == 1 and isinstance(steps[0], Collection):
            self.steps = steps[0]
        else:
            self.steps = list(steps)
        self.loops = {step: None for step in self.steps}
        self.initial_data = data

    def start(self):
        if len(self.steps) == 0:
            raise ValueError("No steps to start")
        self.attach_outputs()
        for step in self.steps:
            self.loops[step] = step.start()

    async def join(self):
        for step in self.steps:
            await step.join()

    def attach_outputs(self):
        for i in range(len(self.steps) - 1):
            completed_items = self.steps[i]
            next_queue = self.steps[i + 1]
            completed_items.attach(next_queue)
        