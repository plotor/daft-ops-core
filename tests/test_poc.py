import daft
from daft import col, DataType

from daft_ops.cls import StatefulExecSpec
from daft_ops.op.text.trans import Translator, translate


# @pytest.fixture
# def translator():
#     return Translator(model_path="/opt/workspace/model/opus-mt-en-zh")
#
#
# def test_stateful_operator_to_udf(translator):
#     udf = to_daft_udf(translator)
#     assert udf is not None
#
#
# def test_translator_row_wise(translator):
#     udf = to_daft_udf(translator)
#
#     df = daft.from_pydict({"text": ["Hello, world!", "How are you?"]})
#
#     df = df.with_column("translated", udf.row_wise_process(daft.col("text")))
#     df.show()
#
#     result = df.to_pydict()
#     assert len(result["translated"]) == 2
#     assert all(isinstance(t, str) for t in result["translated"])
#
#
# def test_translator_batch_wise(translator):
#     udf = to_daft_udf(translator)
#
#     df = daft.from_pydict(
#         {
#             "texts": [
#                 ["Hello", "World"],
#                 ["Test", "Example"],
#             ]
#         }
#     )
#
#     df = df.with_column("translated_batch", udf.batch_wise_process(daft.col("texts")))
#     df.show()
#
#     result = df.to_pydict()
#     assert len(result["translated_batch"]) == 2


def test_create_udf_class():
    df = daft.from_pydict({"text": ["Hello, world!"]})

    # Translator 应该是延迟初始化的
    translator = Translator(model_path="/opt/workspace/model/opus-mt-en-zh")

    df = df.with_column(
        "translated",
        translate(translator, col("text"), StatefulExecSpec(
            return_dtype=DataType.string(),
            num_cpus=0.1
        ))

    )

    df.show()
