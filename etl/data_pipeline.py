import asyncio
from etl.pipeline_step import PipelineStep
from typing import Optional, Union, Dict, List
from collections.abc import Collection
from queue import Queue


class DataPipeline():
    """A collection of pipeline steps to extract, transform, and load data.

    Attributes
    ----------
    steps : list of PipelineStep
        A list of PipelineStep instances with a `process_batch` method to process data

    data : collection
        Initial data to send to the first pipeline step for processing.
    """
    def __init__(self, *steps: Optional[Union[List[PipelineStep], PipelineStep]], data=[]):
        if len(steps) == 1 and isinstance(steps[0], Collection):
            self.steps = steps[0]
        else:
            self.steps = list(steps)
        self.loops: Dict[PipelineStep, Optional[asyncio.Task]] = {step: None for step in self.steps}
        self.initial_data = data
        self.results_queue = Queue()

    async def run(self):
        """
        Coroutine for when all steps have been initialized and `initial_data` is set.

        This coroutine manages starting the steps, waiting for all of them to complete, and returning the results.
        """
        self.start()
        try:
            await self.join()
        finally:
            for step in self.steps:
                step.stop()
        return self.results

    def start(self):
        """Attaches each of the steps and starts their loops to check for new data.
        """
        if len(self.steps) == 0:
            raise ValueError("No steps to start")
        self.attach_outputs()
        for datum in self.initial_data:
            self.steps[0].data.put(datum)
        for step in self.steps:
            self.loops[step] = step.start()
    
    @property
    def done(self):
        """Returns True if all of the steps are done."""
        return all([step.done for step in self.steps])

    async def join(self):
        """Waits for all running steps to process all incoming data, and then stops their loops to check for new data."""
        for step in self.steps:
            await step.join()
            step.stop()

    def attach_outputs(self):
        """Adds the input to each step to the list of outputs of the step before it."""
        for i in range(len(self.steps) - 1):
            completed_items = self.steps[i]
            next_queue = self.steps[i + 1]
            completed_items.attach(next_queue)
        self.steps[-1].attach(self.results_queue)

    @property
    def results(self):
        """The results of the pipeline.

        This returns the current output of the last step in the pipeline.
        """
        return list(self.results_queue.queue)
        