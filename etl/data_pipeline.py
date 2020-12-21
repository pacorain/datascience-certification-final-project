from etl.pipeline_step import PipelineStep
from typing import Collection, Optional, Union


class DataPipeline():
    def __init__(self, *steps: Optional[Union[Collection[PipelineStep], PipelineStep]]):
        if len(steps) == 1 and hasattr(steps[0], '__iter__'):
            self.steps = steps[0]
        else:
            self.steps = steps
        self.loops = {step: None for step in steps}

    def start(self, block=False):
        if len(self.steps) == 0:
            raise ValueError("No steps to start")
        for step in self.steps:
            self.loops[step] = step.start()

        
        