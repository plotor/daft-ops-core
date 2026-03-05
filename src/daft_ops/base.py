from abc import ABC

from daft.datatype import DataTypeLike


class ExecSpec(ABC):
    return_dtype: DataTypeLike | None = None,
    unnest: bool = False,

    def __init__(self, return_dtype: DataTypeLike | None = None, unnest: bool = False):
        self.return_dtype = return_dtype
        self.unnest = unnest


class Operator(ABC):

    def __init__(self):
        pass

    def name(self) -> str | None:
        return None
