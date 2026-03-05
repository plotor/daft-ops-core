from __future__ import annotations

import functools
from abc import ABCMeta
from typing import Any, Callable, TypeVar
from typing import Literal

from daft.datatype import DataType
from daft.datatype import DataTypeLike
from daft.udf.udf_v2 import Func, ClsBase

from daft_ops.base import ExecSpec, Operator


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


class StatefulOperatorProxy:
    def __init__(
            self,
            op_cls: type[StatefulOperator],
            init_args: tuple = (),
            init_kwargs: dict | None = None,
    ):
        self._op_cls = op_cls
        self._init_args = init_args
        self._init_kwargs = init_kwargs or {}
        self._instance: StatefulOperator | None = None

    def __call__(self, *args, **kwargs):
        exec_spec = kwargs.pop('exec_spec', None)
        final_args = list(args)

        if exec_spec is None and len(final_args) > 0:
            last_arg = final_args[-1]
            if isinstance(last_arg, StatefulExecSpec):
                exec_spec = final_args.pop()

        wrapper = _wrap(self, exec_spec)

        if len(final_args) == 0:
            return wrapper()
        elif len(final_args) == 1:
            return wrapper(final_args[0])
        else:
            return wrapper(*final_args)

    def _get_instance(self) -> StatefulOperator:
        if self._instance is None:
            self._instance = self._op_cls(*self._init_args, **self._init_kwargs)
        return self._instance


class StatefulOperatorMeta(ABCMeta):

    def __new__(mcs, name: str, bases: tuple, namespace: dict):
        cls = super().__new__(mcs, name, bases, namespace)

        if bases and not namespace.get('_is_base_class', False):
            @classmethod
            def with_init_args(cls, *args, **kwargs) -> StatefulOperatorProxy:
                return StatefulOperatorProxy(cls, args, kwargs)

            cls.with_init_args = with_init_args

        return cls


class StatefulOperator(Operator, metaclass=StatefulOperatorMeta):
    def __init__(self):
        super().__init__()

    def row_wise_process(self, *args, **kwargs):
        raise NotImplementedError()

    def batch_wise_process(self, *args, **kwargs):
        raise NotImplementedError()


T = TypeVar("T")


class StatefulOperatorWrapper(ClsBase[T]):
    def __init__(
            self,
            operator_cls: type[StatefulOperator],
            init_args: tuple,
            init_kwargs: dict,
            exec_spec: StatefulExecSpec,
    ):
        self._operator_cls = operator_cls
        self._init_args = init_args
        self._init_kwargs = init_kwargs
        self._exec_spec = exec_spec
        self._daft_local_instance: T | None = None

    def __call__(self, *args, **kwargs):
        if self._exec_spec.batch_size is None:
            return self.row_wise_process(*args, **kwargs)
        else:
            return self.batch_wise_process(*args, **kwargs)

    def __getstate__(self) -> dict[str, Any]:
        state = self.__dict__.copy()
        if "_daft_local_instance" in state:
            del state["_daft_local_instance"]
        return state

    def __setstate__(self, state: dict[str, Any]) -> None:
        self.__dict__.update(state)
        self._daft_local_instance = None

    def _daft_get_instance(self) -> T:
        if self._daft_local_instance is None:
            self._daft_local_instance = self._operator_cls(*self._init_args, **self._init_kwargs)
        return self._daft_local_instance

    def __getattr__(self, name: str) -> Func:
        attr = getattr(self._operator_cls, name, None)
        if attr is None or not callable(attr):
            raise AttributeError(f"'{name}' is not a method of {self._operator_cls.__name__}")

        return self._wrap_method(attr, name)

    def _wrap_method(self, method: Callable, method_name: str) -> Func:
        is_batch = method_name == "batch_wise_process"

        return_dtype = self._exec_spec.return_dtype
        if return_dtype is None:
            return_dtype = DataType.string()

        @functools.wraps(method)
        def wrapped_method(instance, *args, **kwargs):
            return method(instance, *args, **kwargs)

        setattr(wrapped_method, "_daft_return_dtype", return_dtype)
        setattr(wrapped_method, "_daft_unnest", self._exec_spec.unnest)
        setattr(wrapped_method, "_daft_batch_method", is_batch)
        setattr(wrapped_method, "_daft_batch_size", self._exec_spec.batch_size)
        setattr(wrapped_method, "_daft_max_retries", self._exec_spec.max_retries)
        setattr(wrapped_method, "_daft_on_error", self._exec_spec.on_error)

        return Func._from_method(
            cls_=self,
            method=wrapped_method,
            gpus=self._exec_spec.num_gpus or 0.0,
            use_process=self._exec_spec.use_process,
            max_concurrency=self._exec_spec.max_concurrency,
            max_retries=self._exec_spec.max_retries,
            on_error=self._exec_spec.on_error,
        )


def _wrap(
        operator: type[StatefulOperator] | StatefulOperatorProxy,
        exec_spec: StatefulExecSpec | None = None,
) -> StatefulOperatorWrapper:
    if exec_spec is None:
        exec_spec = StatefulExecSpec()

    if isinstance(operator, StatefulOperatorProxy):
        return StatefulOperatorWrapper(
            operator_cls=operator._op_cls,
            init_args=operator._init_args,
            init_kwargs=operator._init_kwargs,
            exec_spec=exec_spec,
        )

    return StatefulOperatorWrapper(
        operator_cls=operator,
        init_args=(),
        init_kwargs={},
        exec_spec=exec_spec,
    )
