import asyncio
from etl.pipeline_step import PipelineStep
from typing import Optional, Union, Dict
from collections.abc import Collection
from queue import Queue


class DataPipeline():
    def __init__(self, *steps: Optional[Union[Collection[PipelineStep], PipelineStep]], data=[]):
        if len(steps) == 1 and isinstance(steps[0], Collection):
            self.steps = steps[0]
        else:
            self.steps = list(steps)
        self.loops: Dict[PipelineStep, Optional[asyncio.Task]] = {step: None for step in self.steps}
        self.initial_data = data
        self.results_queue = Queue()

    def start(self):
        if len(self.steps) == 0:
            raise ValueError("No steps to start")
        self.attach_outputs()
        for datum in self.initial_data:
            self.steps[0].data.put(datum)
        for step in self.steps:
            self.loops[step] = step.start()

    async def join(self):
        for step in self.steps:
            await step.join()
            step.stop()

    def attach_outputs(self):
        for i in range(len(self.steps) - 1):
            completed_items = self.steps[i]
            next_queue = self.steps[i + 1]
            completed_items.attach(next_queue)
        self.steps[-1].attach(self.results_queue)

    @property
    def results(self):
        return list(self.results_queue.queue)
        