from abc import ABC, abstractmethod
import asyncio
from asyncio.futures import Future
from queue import Queue, Empty
from typing import Iterable, AsyncGenerator


class PipelineStep(ABC):
    """An abstract step in a data pipeline. Still figuring out how this actually works...
    
    Attributes
    ----------
    max_batch_size : int
        The maximum number of records to process at once.

        When process_batch is called, it is called with a list that will be no bigger than this variable.

        Defaults to 100, which is suitable for APIs that support making multiple queries at once, such as 
        databases. However, if an API only supports one request at a time, this should be set to 1.
    
    drop_duplicates : bool
        If True, drop records when is_duplicate returns True.

    async_batches : bool
        Allows more than one batch to be processed at a time. Good for HTTP servers, which often utilize 
        parallelism; but not so much for things like databases, where parallel connections may yield no
        benefit.
    """

    max_batch_size = 100
    drop_duplicates = True
    async_batches = False

    def __init__(self):
        self.data = Queue()
        self.outputs = []
        self.running = False
        self._duplicate_cache = set()
        self._task: asyncio.Task = None
        self._all_tasks = []
    
    @abstractmethod
    async def process_batch(self, batch: Iterable) -> AsyncGenerator:
        pass

    def start(self):
        """Starts processing data in the step's data queue."""
        event_loop = asyncio.get_event_loop()
        task = event_loop.create_task(self._loop(event_loop))
        self.running = True
        return task

    def stop(self):
        self.running = False

    async def _loop(self, loop):
        while loop.is_running():
            if self.async_batches:
                task = loop.create_task(self._create_batch())
                self._all_tasks.append(task)
            elif self._task is None or self._task.done:
                task = loop.create_task(self._create_batch())
                self._task = task
                self._all_tasks.append(task)
            await asyncio.sleep(0.1)

    async def _create_batch(self):
        batch = []
        for _ in range(self.max_batch_size or self.data.qsize()):
            try:
                batch.append(self.data.get_nowait())
            except Empty:
                break
        if batch:
            async for result_datum in self.process_batch(batch):
                for output in self.outputs:
                    output.put(result_datum)


    def put(self, record):
        """Puts a single record in the pipeline step to be processed later.

        Parameters
        ----------
        record : any
            Record of type that can be accepted by overriden `process_batch` method.
        """
        if self.drop_duplicates:
            if self.is_duplicate(record):
                return
        self.data.put(record)
    
    def is_duplicate(self, record):
        """Method to detirmine if a record has been `put` before when `drop_duplicates` is True.

        By default, object hashes are stored, and checked against incoming object hashes. This method
        can be overriden to change the behavior that determins whether or not a record is a duplicate.
        """
        if hash(record) not in self._duplicate_cache:
            self._duplicate_cache.add(hash(record))
            return False
        return True

    def attach(self, output):
        """Attach a queue to this pipeline step's output.

        Parameters
        ----------
        output : PipelineStep, Queue
            The output attached. Any object yielded by `process_batch` will be sent to this queue or
            pipeline step.
        """
        self.outputs.append(output)

    @property
    def done(self):
        """Checks if the queue is empty and all batches are complete.
        
        Parameters
        ----------
        ignore_exeptions : bool, default False
            Adjusts behavior when an exception is encountered in one of the tasks. By default, the
            exception is raised. Set to `True` to bypass exceptions.
        """
        for task in self._all_tasks:
            if not task.done():
                return False
            task.result()  # Raise task exceptions
        return self.data.empty()

    async def join(self):
        """Blocks execution until pipeline_step.done is true"""
        while not self.done:
            await asyncio.sleep(0.1)