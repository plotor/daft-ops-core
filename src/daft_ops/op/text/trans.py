import logging
import time

from daft_ops.cls import StatefulOperator

logger = logging.getLogger(__name__)


class Translator(StatefulOperator):

    def __init__(self, model_path: str):
        super().__init__()
        self.model_path = model_path
        print(f"Initializing Translator, and load model from {self.model_path}")
        self._model = f"Model loaded from {self.model_path}"

    def row_wise_process(self, id: int, sentence: str) -> str:
        print(f"{id}: \ttranslating: {sentence}")
        time.sleep(0.1)
        return f"Translated: {sentence}"

    def batch_wise_process(self, ids: list[int], sentences: list[str]) -> list[str]:
        print(f"Translating {len(sentences)} sentences")
        return [self.row_wise_process(id, sentence) for id, sentence in zip(ids, sentences)]

    def name(self) -> str | None:
        return "Translator"

# def translate(
#         translator: Union[type[Translator], _LazyStatefulOperator],
#         sentences,
#         exec_spec: StatefulExecSpec | None = None,
# ):
#     return _wrap(
#         operator=translator,
#         exec_spec=exec_spec,
#     )(sentences)
