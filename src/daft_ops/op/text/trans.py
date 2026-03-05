import logging

from daft import Expression

from daft_ops.cls import StatefulOperator, StatefulExecSpec

logger = logging.getLogger(__name__)


class Translator(StatefulOperator):
    def __init__(self, model_path: str):
        super().__init__()

        print(f"Initializing Translator, and load model from {model_path}")

    def row_wise_process(self, sentence, exec_spec: StatefulExecSpec):
        logger.info("Translating: '%s'", sentence)
        return f"Translated: {sentence}"

    def batch_wise_process(self, sentences, exec_spec: StatefulExecSpec):
        logger.info("Translating %d sentences", len(sentences))
        return [self.row_wise_process(sentence) for sentence in sentences]

    def name(self) -> str | None:
        return "Translator"


def translate(translator: Translator, sentence, exec_spec: StatefulExecSpec) -> Expression:
    return translator.row_wise_process(sentence, exec_spec)


def translates(translator: Translator, sentences, exec_spec: StatefulExecSpec) -> Expression:
    return translator.batch_wise_process(sentences, exec_spec)
