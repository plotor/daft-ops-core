import daft
from daft import col, DataType
from daft_ops.cls import StatefulExecSpec
from daft_ops.op.text.trans import Translator

df = daft.from_pydict({"id": list(range(1024)), "text": ["Hello, world!"] * 1024})

translator = Translator.with_init_args(model_path="/opt/workspace/model/opus-mt-en-zh")
df = df.with_column(
    "result",
    translator(
        col("id"),
        col("text"),
        exec_spec=StatefulExecSpec(
            return_dtype=DataType.string(),
            num_cpus=0.1,
            max_concurrency=8,
            batch_size=None,
        )
    )
)

df.collect()
