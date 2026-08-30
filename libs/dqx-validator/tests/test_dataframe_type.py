"""_is_dataframe accepts classic AND Spark Connect DataFrames.

On Databricks serverless, spark.table(...) returns
pyspark.sql.connect.dataframe.DataFrame, which is not a subclass of the classic
pyspark.sql.DataFrame — a plain isinstance check rejects it with
"input_df must be a pyspark.sql.DataFrame".
"""
import pytest

from dqx_validator.engine import _is_dataframe


class DataFrame:  # noqa: N801 - deliberately mimics pyspark.sql.connect...DataFrame
    """Not a subclass of the (stubbed) classic DataFrame, but walks like one."""

    def __init__(self):
        self.schema = object()
        self.columns = ["a", "b"]

    def withColumn(self, *_a, **_k):
        return self


def test_classic_dataframe_accepted(FakeDFFactory):
    df = FakeDFFactory(["a"], spark=None)
    assert _is_dataframe(df) is True


def test_connect_style_dataframe_accepted():
    assert _is_dataframe(DataFrame()) is True


def test_object_named_dataframe_without_withColumn_rejected():
    class DataFrame:  # noqa: N801
        schema = object()
        columns = []

    assert _is_dataframe(DataFrame()) is False


@pytest.mark.parametrize("bad", [None, "df", 123, object(), [], {}])
def test_non_dataframe_rejected(bad):
    assert _is_dataframe(bad) is False


def test_duck_typed_but_wrong_class_name_rejected():
    class NotADataFrame:
        schema = object()
        columns = []

        def withColumn(self):
            return self

    assert _is_dataframe(NotADataFrame()) is False
