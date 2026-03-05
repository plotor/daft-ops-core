from abc import ABC, abstractmethod
from typing import Literal

from daft.datatype import DataTypeLike

from daft_ops.base import ExecSpec


class StatefulExecSpec(ExecSpec):
    num_cpus: float | None = None
    num_gpus: float | None = None
    memory_bytes: int | None = None
    use_process: bool | None = None
    max_concurrency: int | None = None

    batch_size: int | None = None

    max_retries: int | None = None
    on_error: Literal["raise", "log", "ignore"] | None = None

    def __init__(
            self,
            return_dtype: DataTypeLike | None = None,
            unnest: bool = False,
            num_cpus: float | None = None,
            num_gpus: float | None = None,
            memory_bytes: int | None = None,
            use_process: bool | None = None,
            max_concurrency: int | None = None,
            batch_size: int | None = None,
            max_retries: int | None = None,
            on_error: Literal["raise", "log", "ignore"] | None = None,
    ):
        super().__init__(return_dtype, unnest)
        self.num_cpus = num_cpus
        self.num_gpus = num_gpus
        self.memory_bytes = memory_bytes
        self.use_process = use_process
        self.max_concurrency = max_concurrency
        self.batch_size = batch_size
        self.max_retries = max_retries
        self.on_error = on_error


class Operator(ABC):
    def __init__(self):
        pass

    def name(self) -> str | None:
        return None


class StatefulOperator(Operator):
    def __init__(self):
        super().__init__()

    @abstractmethod
    def row_wise_process(self, *args, **kwargs):
        pass

    @abstractmethod
    def batch_wise_process(self, *args, **kwargs):
        pass
