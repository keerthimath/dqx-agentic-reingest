"""
Hermetic test setup for dqx-validator.

The library imports pyspark + databricks SDKs at module load. None of that is
needed to test the rule-retrieval / naming / row-id logic, so we install thin
stand-ins in ``sys.modules`` *before* ``dqx_validator`` is imported. Anything a
test actually exercises against Spark is replaced with a recording fake.
"""
import os
import sys
import types

_HERE = os.path.dirname(__file__)
sys.path.insert(0, os.path.abspath(os.path.join(_HERE, "..")))


# --------------------------------------------------------------------------- #
# Stub the heavy imports.
# --------------------------------------------------------------------------- #
class _Any:
    """Accept any attribute access / call / comparison and keep chaining."""

    def __getattr__(self, _name):
        return _Any()

    def __call__(self, *_a, **_k):
        return _Any()

    def __eq__(self, _other):
        return _Any()

    def __ne__(self, _other):
        return _Any()

    def __hash__(self):
        return 0

    def __iter__(self):
        return iter(())


class _StubDataFrame:
    """Base class so ``isinstance(df, DataFrame)`` passes for our fakes."""


def _install_stub(name, **attrs):
    mod = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(mod, k, v)
    sys.modules[name] = mod
    return mod


_pyspark = _install_stub("pyspark")
_pyspark_sql = _install_stub("pyspark.sql", SparkSession=_Any(), DataFrame=_StubDataFrame)
_pyspark_sql.functions = _install_stub("pyspark.sql.functions")
# Every F.<fn>(...) returns a chainable dummy expression.
for _fn in (
    "col lit to_json sha2 concat_ws coalesce current_timestamp row_number "
    "map_keys create_map element_at transform array array_union explode count "
    "expr trim lower upper"
).split():
    setattr(_pyspark_sql.functions, _fn, _Any())
_install_stub("pyspark.sql.window", Window=_Any())
_pyspark.sql = _pyspark_sql

_install_stub("databricks")
_install_stub("databricks.sdk", WorkspaceClient=_Any)
_install_stub("databricks.labs")
_install_stub("databricks.labs.dqx")
_install_stub("databricks.labs.dqx.engine", DQEngine=_Any)
_install_stub("databricks.labs.dqx.config", ExtraParams=_Any)


# --------------------------------------------------------------------------- #
# Recording fakes for the flow test.
# --------------------------------------------------------------------------- #
import pytest  # noqa: E402

from dqx_validator import engine as engine_mod  # noqa: E402
from dqx_validator.engine import Validator  # noqa: E402


class FakeRow(dict):
    def __getitem__(self, k):
        return super().__getitem__(k)


class FakeWriter:
    def __init__(self, df):
        self._df = df

    def format(self, *_a, **_k):
        return self

    def mode(self, *_a, **_k):
        return self

    def option(self, *_a, **_k):
        return self

    def saveAsTable(self, name):
        self._df._spark.saved_tables.append(name)
        self._df._spark.last_saved_columns = list(self._df.columns)


class FakeDF(_StubDataFrame):
    """Tracks withColumn names and write targets; never does IO."""

    def __init__(self, columns, spark, *, empty=False, count=0):
        self.columns = list(columns)
        self._spark = spark
        self._empty = empty
        self._count = count
        self.added_columns = []

    def withColumn(self, name, _expr):
        new = FakeDF(self.columns + [name], self._spark, empty=self._empty, count=self._count)
        new.added_columns = self.added_columns + [name]
        return new

    def isEmpty(self):
        return self._empty

    def count(self):
        return self._count

    def limit(self, _n):
        return self

    def createOrReplaceTempView(self, _n):
        return None

    @property
    def write(self):
        return FakeWriter(self)


class FakeQuery:
    def __init__(self, rows, recorder):
        self._rows = rows
        self._rec = recorder

    def filter(self, *_a, **_k):
        self._rec["filters"] += 1
        return self

    def select(self, *_a, **_k):
        self._rec["selects"] += 1
        return self

    def collect(self):
        return [FakeRow(r) for r in self._rows]


class FakeCatalog:
    def __init__(self, exists=True):
        self._exists = exists

    def tableExists(self, _name):
        return self._exists

    def dropTempView(self, _n):
        return None


class FakeReader:
    def __init__(self, rows, recorder):
        self._rows = rows
        self._rec = recorder

    def table(self, _fqn):
        return FakeQuery(self._rows, self._rec)


class FakeSpark:
    def __init__(self, rule_rows=(), table_exists=True):
        self.rule_rows = list(rule_rows)
        self.query_calls = {"filters": 0, "selects": 0}
        self.saved_tables = []
        self.last_saved_columns = None
        self.catalog = FakeCatalog(table_exists)

    @property
    def read(self):
        return FakeReader(self.rule_rows, self.query_calls)


class FakeDQEngine:
    def __init__(self, valid_df, invalid_df):
        self._pair = (valid_df, invalid_df)
        self.calls = []

    def apply_checks_by_metadata_and_split(self, df, checks):
        self.calls.append((df, checks))
        return self._pair

    def apply_checks_and_split(self, df, checks):
        self.calls.append((df, checks))
        return self._pair


@pytest.fixture
def make_validator():
    """Build a Validator with __init__ bypassed and fakes wired in."""

    def _factory(*, rule_rows=(), table_exists=True, valid_df=None, invalid_df=None):
        v = Validator.__new__(Validator)
        v.env = "dev"
        v.rules_table = "main.dqx_app.dq_quality_rules"
        v.universal_rules_table = "main.dqx_app.dq_universal_quality_rules"
        v._quarantine_override = None
        v.quarantine_schema = "quarantine"
        v.spark = FakeSpark(rule_rows=rule_rows, table_exists=table_exists)
        if valid_df is not None or invalid_df is not None:
            v.dq_engine = FakeDQEngine(valid_df, invalid_df)
        return v

    return _factory


@pytest.fixture
def engine():
    return engine_mod


@pytest.fixture
def FakeDFFactory():
    return FakeDF


@pytest.fixture
def flow(make_validator):
    """(validator, input_df, valid_df, invalid_df) with DQX's split faked.

    ``columns`` are the input columns; ``invalid_empty`` / ``invalid_count``
    control the fake 'bad rows' split.
    """

    def _factory(*, columns, rule_rows, invalid_empty, invalid_count=0):
        v = make_validator(rule_rows=rule_rows)
        input_df = FakeDF(columns, v.spark)
        valid_df = FakeDF(columns, v.spark)
        invalid_df = FakeDF(columns, v.spark, empty=invalid_empty, count=invalid_count)
        v.dq_engine = FakeDQEngine(valid_df, invalid_df)
        return v, input_df, valid_df, invalid_df

    return _factory
