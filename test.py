from etl import PipelineStep, DataPipeline, pipeline_step
from typing import AsyncGenerator
from queue import Queue
import unittest
from unittest.mock import Mock
import asyncio

def asynctest(coro):
    def wrapper(test_case):
        asyncio.run(coro(test_case))
    return wrapper

class SimplePipelineStep(PipelineStep):
    async def process_batch(self, batch) -> AsyncGenerator:
        yield 1
        yield 2
        yield 3

class EchoPipelineStep(PipelineStep):
    async def process_batch(self, batch) -> AsyncGenerator:
        for record in batch:
            yield record

class SquarePipelineStep(PipelineStep):
    async def process_batch(self, batch) -> AsyncGenerator:
        for record in batch:
            yield record ** 2

class TestPipelineStep(unittest.TestCase):
    def test_pipeline_step_defaults(self):
        step = SimplePipelineStep()
        self.assertEqual(step.max_batch_size, 100)
        self.assertFalse(step.async_batches)
        self.assertTrue(step.drop_duplicates)

    def test_undefined_pipeline_step_raises_typeerror(self):
        self.assertRaises(TypeError, PipelineStep)

    def test_pipeline_step_attaches_output(self):
        queue = Mock(Queue)
        step = SimplePipelineStep()
        step.attach(queue)
        self.assertIn(queue, step.outputs)

    @asynctest
    async def test_simple_pipeline_output(self):
        output = Queue()
        step = SimplePipelineStep()
        step.put("any record")
        step.attach(output)
        step.start()
        await step.join()
        for i in range(1, 4):
            datum = output.get_nowait()
            self.assertEqual(datum, i)

    @asynctest
    async def test_duplicates_are_dropped(self):
        step = SimplePipelineStep()
        for i in list(range(50)):
            step.put(i)
            self.assertEqual(i + 1, step.data.qsize())
        for i in range(50):
            step.put(i)
            self.assertEqual(50, step.data.qsize())

    @asynctest
    async def test_batch_sizes(self):
        class BatchSizeTester(PipelineStep):
            async def process_batch(self, batch):
                yield len(batch)
        output = Queue()
        pipeline_step = BatchSizeTester()
        pipeline_step.attach(output)
        for i in range(250):
            pipeline_step.put(i)
        pipeline_step.start()
        await pipeline_step.join()
        self.assertEqual([100, 100, 50], list(output.queue)) 

    @asynctest
    async def test_step_raises_error_when_batch_rose_error(self):
        class PoorlyWrittenStep(PipelineStep):
            async def process_batch(self, batch):
                yield 1/0
        pipeline_step = PoorlyWrittenStep()
        pipeline_step.put('placeholder')
        self.assertFalse(pipeline_step.done)
        pipeline_step.start()
        try:
            await pipeline_step.join()
            self.fail("Join did not raise exception")
        except Exception as e:
            self.assertIsInstance(e, ZeroDivisionError)

class TestDataPipeline(unittest.TestCase):
    def test_pipeline_step_started(self):
        step = Mock(PipelineStep)
        pipeline = DataPipeline(step)
        pipeline.start()
        step.start.assert_called()

    def test_empty_pipeline_raises_error(self):
        pipeline = DataPipeline()
        self.assertRaises(ValueError, pipeline.start)

    def test_list_arg_is_not_unpacked(self):
        step1 = SimplePipelineStep()
        step2 = SquarePipelineStep()
        step3 = SquarePipelineStep()
        pipeline1 = DataPipeline(step1, step2, step3)
        pipeline2 = DataPipeline([step1, step2, step3])
        self.assertEqual(pipeline1.steps, pipeline2.steps)
    
    @asynctest
    async def test_step_1_attached_to_step_2(self):
        step1 = SimplePipelineStep()
        step2 = SquarePipelineStep()
        pipeline = DataPipeline(step1, step2)
        pipeline.start()
        self.assertIn(step2, step1.outputs)

    @asynctest
    async def test_step_1_results_in_step_2(self):
        step1 = SimplePipelineStep()
        step1.data.put(1)
        step2 = EchoPipelineStep()
        step2.attach(Queue())
        pipeline = DataPipeline(step1, step2)
        pipeline.start()
        await pipeline.join()
        self.assertEqual([1, 2, 3], list(step2.outputs[0].queue))
        
    @asynctest
    async def test_results_are_data_of_final_step(self):
        step1 = SimplePipelineStep()
        step2 = SquarePipelineStep()
        step3 = SquarePipelineStep()
        pipeline = DataPipeline(step1, step2, step3, data=[1])
        pipeline.start()
        await pipeline.join()
        results = pipeline.results
        self.assertEqual(results, [1, 16, 81])

    
if __name__ == '__main__':
    unittest.main()