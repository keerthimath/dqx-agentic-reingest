import logging
import json
import re
import io
import copy
import hashlib
from contextlib import redirect_stdout
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.config import ExtraParams
from databricks.sdk import WorkspaceClient

from . import config

logger = logging.getLogger(__name__)

# On Databricks serverless / Spark Connect, spark.table(...) returns a
# pyspark.sql.connect.dataframe.DataFrame, which is NOT a subclass of the
# classic pyspark.sql.DataFrame in most pyspark versions. Accept both so
# isinstance() checks don't reject a perfectly good Connect DataFrame.
_DATAFRAME_TYPES: tuple = (DataFrame,)
try:  # pragma: no cover - depends on runtime
    from pyspark.sql.connect.dataframe import DataFrame as _ConnectDataFrame

    _DATAFRAME_TYPES = (DataFrame, _ConnectDataFrame)
except Exception:
    pass


def _is_dataframe(obj) -> bool:
    """True for a classic or Spark Connect DataFrame."""
    if isinstance(obj, _DATAFRAME_TYPES):
        return True
    # Last-resort duck check: some runtimes ship yet another DataFrame class
    # (e.g. under pyspark.sql.classic). Treat anything that walks like one as one.
    return (
        type(obj).__name__ == "DataFrame"
        and hasattr(obj, "schema")
        and hasattr(obj, "columns")
        and callable(getattr(obj, "withColumn", None))
    )


# DQX writes per-row check results into these columns on the "invalid" split.
# We name them plainly (no leading underscore is required by DQX). Everything
# this package adds on top uses a leading underscore to stay clear of real
# data columns.
ERROR_COLUMN = "_error"
WARNING_COLUMN = "_warning"
GENERATED_AT_COLUMN = "_generated_at"
ROW_ID_COLUMN = "_row_id"
ROW_ID_KEYS_COLUMN = "_row_id_keys"  # CSV of the business-key columns behind _row_id ("" = hashed all columns)

# Separator + null sentinel for the _row_id hash. Chosen so they can't collide
# with real key values (unit separator, and an explicit null marker).
_ROW_ID_SEP = "\x1f"
_ROW_ID_NULL = "\x00NULL\x00"


# --------------------------------------------------------------------------- #
# Pure helpers (no Spark) — unit-tested directly in tests/.
# --------------------------------------------------------------------------- #
def checks_from_rule_rows(rows: list[dict], source: str = "<rules table>") -> list:
    """
    Turn collected rule rows into a list of DQX check dicts.

    Each row is a mapping with at least ``rule_id`` and ``check_json`` (a JSON
    string, as produced by ``to_json(check)``). Rules flagged
    ``user_metadata.monitor_only = "true"`` are dropped — they must never
    quarantine data. Empty or unparseable payloads raise ``ValueError``.
    """
    checks = []
    monitor_only_count = 0
    for row in rows:
        check_json = row.get("check_json")
        if not check_json:
            raise ValueError(
                f"Rule '{row.get('rule_id')}' in '{source}' has empty 'check' payload."
            )
        try:
            check = json.loads(check_json)
        except Exception as e:
            raise ValueError(
                f"Rule '{row.get('rule_id')}' in '{source}' has invalid check payload: {e}"
            ) from e

        user_metadata = check.get("user_metadata") or {} if isinstance(check, dict) else {}
        if str(user_metadata.get("monitor_only", "")).strip().lower() == "true":
            monitor_only_count += 1
            logger.info("Skipping monitor_only rule '%s' (will not quarantine).", row.get("rule_id"))
            continue

        checks.append(check)

    if monitor_only_count:
        logger.info("Excluded %d monitor_only rule(s) from enforcement.", monitor_only_count)
    logger.info("Loaded %d approved check(s) from %s.", len(checks), source)
    return checks


def resolve_row_id_keys(row_id_columns, scope_columns, available_columns):
    """
    Decide which columns feed the ``_row_id`` hash and the ``_row_id_keys`` CSV.

    - ``row_id_columns`` given (str or list): those columns, validated against
      ``available_columns``; ``_row_id_keys`` is their comma-join.
    - omitted / falsy: every column in ``scope_columns`` is hashed and
      ``_row_id_keys`` is ``""`` (no natural key → not reingestable as a MERGE
      key), with a warning.

    Returns ``(columns, keys_csv)``.
    """
    if row_id_columns:
        cols = [row_id_columns] if isinstance(row_id_columns, str) else list(row_id_columns)
        cols = [c.strip() for c in cols if isinstance(c, str) and c.strip()]
        if not cols:
            raise ValueError("row_id_columns must contain at least one column name")
        missing = [c for c in cols if c not in available_columns]
        if missing:
            raise ValueError(
                f"row_id_columns not present in input_df: {missing}. "
                f"Available: {list(available_columns)}"
            )
        return cols, ",".join(cols)

    cols = list(scope_columns)
    logger.warning(
        "Validation invoked without row_id_columns; %s will hash all %d input "
        "columns. Pass row_id_columns=<business key> so the reingest review "
        "queue keys rows on a stable natural key.",
        ROW_ID_COLUMN, len(cols),
    )
    return cols, ""


class Validator:
    def __init__(
        self,
        spark: SparkSession = None,
        env: str = None,
        quarantine_table: str = None,
        rules_table: str = None,
    ):
        self.spark = spark or SparkSession.builder.getOrCreate()
        if not env:
            raise ValueError(
                "env is required, e.g. Validator(env='dev'). "
                "Pass it from a Databricks job or notebook parameter."
            )
        self.env = config.normalize_env(env)

        # Explicit override args win; otherwise every name is resolved per-env
        # from tables.yaml. Expected formats:
        #   quarantine_table: <catalog>.<schema>        e.g. my_catalog.quarantine
        #   rules_table:      <catalog>.<schema>.<table>
        #
        # By default the quarantine table is NOT pinned to `env`: its catalog
        # follows the source table being validated (see
        # _resolve_quarantine_table_name), only the schema is fixed. Passing
        # quarantine_table="<catalog>.<schema>" pins both.
        self._quarantine_override = quarantine_table
        self.quarantine_schema = config.get_quarantine_schema()
        self.quarantine_table = quarantine_table or config.get_quarantine_table(self.env)
        self.rules_table = rules_table or config.get_rules_table(self.env)
        self.universal_rules_table = config.get_universal_rules_table(self.env)

        self.ws = WorkspaceClient()
        # Name DQX's result columns so the quarantine table is just the input
        # schema + _error + _warning (+ the audit columns we add on write).
        self.dq_engine = DQEngine(
            self.ws,
            extra_params=ExtraParams(
                result_column_names={
                    "errors": ERROR_COLUMN,
                    "warnings": WARNING_COLUMN,
                }
            ),
        )

    def approved_checks_for(self, table_fqns: str | list[str]) -> list:
        """
        Public loader for the approved DQX checks attached to one or more
        source table_fqns. Same rows `run_validation` uses to quarantine, so
        the reingest side can re-validate a proposed fix against the exact
        same checks:

            from dqx_validator import Validator
            checks = Validator(env=env).approved_checks_for("main.silver.orders")
            valid_df, still_bad = dq_engine.apply_checks_and_split(fixed_df, checks)
        """
        if isinstance(table_fqns, str):
            table_fqns = [table_fqns]
        if not isinstance(table_fqns, (list, tuple)):
            raise ValueError(
                "approved_checks_for requires a table_fqn string or a list of them, "
                f"got: {type(table_fqns).__name__}"
            )
        normalized = list(dict.fromkeys(
            t.strip() for t in table_fqns if isinstance(t, str) and t.strip()
        ))
        if not normalized:
            raise ValueError("approved_checks_for requires at least one table_fqn")
        return self._load_checks_from_rules_table(normalized)

    def _row_id_column(self, df: DataFrame, row_id_columns, scope_columns: list[str]):
        """
        Build the `_row_id` hash expression and the CSV of key columns behind
        it (`_row_id_keys`, "" when the whole row was hashed).

        Returns (row_id_expr, keys_csv). Column-selection logic is delegated to
        the pure helper `resolve_row_id_keys` (unit-tested without Spark).
        """
        cols, keys_csv = resolve_row_id_keys(
            row_id_columns, scope_columns, available_columns=list(df.columns)
        )
        parts = [
            F.coalesce(F.col(c).cast("string"), F.lit(_ROW_ID_NULL)) for c in cols
        ]
        return F.sha2(F.concat_ws(_ROW_ID_SEP, *parts), 256), keys_csv

    def _load_checks_from_rules_table(self, rule_table_fqns: list[str]) -> list:
        """
        Load approved checks from self.rules_table where:
          - table_fqn IN rule_table_fqns
          - status = 'approved'
        """
        logger.info(f"Loading approved rules from {self.rules_table}...")
        approved_rules_df = (
            self.spark.read.table(self.rules_table)
            .filter(F.col("table_fqn").isin(rule_table_fqns))
            .filter(F.col("status") == F.lit("approved"))
            .select("rule_id", "table_fqn", F.to_json(F.col("check")).alias("check_json"))
        )

        rows = [
            {"rule_id": r["rule_id"], "table_fqn": r["table_fqn"], "check_json": r["check_json"]}
            for r in approved_rules_df.collect()
        ]
        if not rows:
            raise ValueError(
                f"No approved rules found in '{self.rules_table}' for any table_fqn in: {rule_table_fqns}"
            )

        found_fqns = {r["table_fqn"] for r in rows}
        missing_fqns = [t for t in rule_table_fqns if t not in found_fqns]
        if missing_fqns:
            logger.warning(f"No approved rules found for: {missing_fqns}")

        # Parsing / monitor_only filtering is pure — unit-tested without Spark.
        return checks_from_rule_rows(rows, source=self.rules_table)

    def _resolve_quarantine_table_name(self, table_fqns: str | list[str]) -> str:
        """
        Resolve the quarantine table for one or more source table_fqns as:
        <quarantine_catalog>.<quarantine_schema>.<name>

        Single source -> <source_schema>_<source_table>, landing next to the
        data (the schema prefix keeps e.g. bronze.foo and silver.foo apart):
            main.bronze.events
              -> main.quarantine.bronze_events
            other_catalog.silver.orders
              -> other_catalog.quarantine.silver_orders

        Multiple sources (rules span several tables) -> a combined name with a
        cross_table marker, so it is never confused with a single-table
        quarantine. Per-row attribution stays in the _rule_sources column:
            [main.silver.accounts, main.silver.leads]
              -> main.quarantine.silver_cross_table__accounts__leads

        No assumption is made about schema names. Sources must share one
        catalog (a single write target); a cross-catalog set is rejected.
        Passing Validator(quarantine_table="<catalog>.<schema>") pins both
        the catalog and schema regardless of the source(s).
        """
        if isinstance(table_fqns, str):
            table_fqns = [table_fqns]
        fqns = list(dict.fromkeys(f.strip() for f in table_fqns if f and f.strip()))
        if not fqns:
            raise ValueError("at least one source table_fqn is required")

        parsed = []
        for fqn in fqns:
            parts = fqn.split(".")
            if len(parts) != 3 or not all(p.strip() for p in parts):
                raise ValueError(
                    f"table_fqn must be '<catalog>.<schema>.<table>', got: '{fqn}'"
                )
            catalog, schema, table = (p.strip().lower() for p in parts)
            parsed.append((catalog, schema, table))

        catalogs = {c for c, _, _ in parsed}
        if len(catalogs) > 1:
            raise ValueError(
                f"cross-table validation requires all rule sources in one catalog "
                f"(a single quarantine target), got: {sorted(catalogs)}"
            )

        source_catalog = parsed[0][0]
        schemas = {s for _, s, _ in parsed}
        # segment per source table, prefixed by its own schema
        segments = sorted(f"{s}_{t}" for _, s, t in parsed)

        if len(parsed) == 1:
            quarantine_table_name = segments[0]
        elif len(schemas) == 1:
            schema = next(iter(schemas))
            tables = sorted(t for _, _, t in parsed)
            quarantine_table_name = f"{schema}_cross_table__" + "__".join(tables)
        else:
            quarantine_table_name = "cross_table__" + "__".join(segments)

        if len(quarantine_table_name) > 200:
            digest = hashlib.sha1("|".join(segments).encode()).hexdigest()[:8]
            prefix = quarantine_table_name.split("cross_table__", 1)[0]
            quarantine_table_name = f"{prefix}cross_table__{digest}"

        if self._quarantine_override:
            quarantine_parts = self._quarantine_override.split(".")
            if len(quarantine_parts) != 2:
                raise ValueError(
                    f"quarantine_table must be '<catalog>.<schema>', "
                    f"got: '{self._quarantine_override}'"
                )
            quarantine_catalog, quarantine_schema = quarantine_parts
        else:
            quarantine_catalog = source_catalog
            quarantine_schema = self.quarantine_schema

        return f"{quarantine_catalog}.{quarantine_schema}.{quarantine_table_name}"

    def _extract_table_fqns_from_text(self, text: str) -> list[str]:
        """
        Extract catalog.schema.table candidates from plan text. Metadata
        tables this package reads (the rules tables, information_schema) are
        excluded; no assumption is made about data schema names.
        """
        if not text:
            return []
        excluded = {
            self.rules_table.lower(),
            self.universal_rules_table.lower(),
        }
        pattern = r"`?([A-Za-z0-9_]+)`?\.`?([A-Za-z0-9_]+)`?\.`?([A-Za-z0-9_]+)`?"
        out = []
        for c, s, t in re.findall(pattern, text):
            fqn = f"{c}.{s}.{t}"
            if s.lower() == "information_schema" or fqn.lower() in excluded:
                continue
            out.append(fqn)
        # preserve order + unique
        return list(dict.fromkeys(out))

    def _infer_source_table_fqn(self, input_df: DataFrame) -> str:
        """
        Best-effort guess of the single source table behind input_df, read
        from its query plan. Used only as a fallback for universal-rule runs
        where no source table is supplied; pass source_table= to skip it.
        Raises if the plan does not reveal exactly one candidate.
        """
        candidates = []

        # 1) JVM analyzed plan (classic runtime)
        try:
            if hasattr(input_df, "_jdf") and input_df._jdf is not None:
                plan = input_df._jdf.queryExecution().analyzed().toString()
                candidates.extend(self._extract_table_fqns_from_text(plan))
        except Exception:
            pass

        # 2a) Connect plan object string (serverless / spark connect)
        try:
            plan_obj = getattr(input_df, "_plan", None)
            if plan_obj is not None:
                candidates.extend(self._extract_table_fqns_from_text(str(plan_obj)))
        except Exception:
            pass

        # 2b) explain() output
        try:
            buf = io.StringIO()
            with redirect_stdout(buf):
                input_df.explain(mode="simple")
            candidates.extend(self._extract_table_fqns_from_text(buf.getvalue()))
        except Exception:
            pass

        candidates = list(dict.fromkeys(candidates))
        if len(candidates) == 1:
            return candidates[0]

        raise ValueError(
            "Could not determine the source table from input_df on this runtime "
            f"(plan candidates: {candidates or 'none'}). "
            "Pass source_table='<catalog>.<schema>.<table>' to run_universal_rule_validation."
        )

    def _create_quarantine_if_missing(self, target_quarantine_table: str, invalid_df: DataFrame) -> None:
        """
        Create empty Delta quarantine table if missing, preserving schema from invalid_df.
        """
        if self.spark.catalog.tableExists(target_quarantine_table):
            return

        tmp_view = "__dqx_invalid_schema_tmp__"
        invalid_df.limit(0).createOrReplaceTempView(tmp_view)

        self.spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {target_quarantine_table}
            USING DELTA
            AS SELECT * FROM {tmp_view}
            """
        )
        self.spark.catalog.dropTempView(tmp_view)
        logger.info(f"Created quarantine table: {target_quarantine_table}")

    def run_validation(
        self,
        input_df: DataFrame,
        rules: list[str] | str,
        row_id_columns: list[str] | str | None = None,
    ) -> DataFrame:
        """
        Run validation on input_df using approved checks attached to the
        table_fqn value(s) in `rules`.

        `rules` may be a single '<catalog>.<schema>.<table>' string or a list
        of them. Those table_fqns are authoritative and drive the quarantine
        target:
          - one source ->  run_validation(df, "other_catalog.silver.orders")
                           quarantine: other_catalog.quarantine.silver_orders
          - many sources -> run_validation(df, ["main.silver.accounts",
                                                "main.silver.leads"])
                           quarantine: main.quarantine.silver_cross_table__accounts__leads
            All sources must share one catalog (a single quarantine target).

        `row_id_columns` (str or list) names the business/natural key columns
        of the source row. Their hash is written to `_row_id`, which the
        reingest review queue keys every quarantined row on. If omitted,
        `_row_id` hashes every input column and a warning is logged.

        Quarantine table schema = every column of input_df unchanged, plus:
          _row_id        string        - sha2-256 of row_id_columns (or all cols)
          _row_id_keys   string        - CSV of the key columns behind _row_id
          _error         array<struct> - failed error-level checks (DQX)
          _warning       array<struct> - failed warn-level checks (DQX)
          _generated_at  timestamp     - when this quarantine row was written
          _data_source   string        - source table_fqn (CSV if cross-table)
          _rule_sources  string        - all rule table_fqns applied (CSV)
          _is_cross_table boolean      - True when rules spanned >1 table
        New input columns on later runs are merged in (mergeSchema).
        """
        if not _is_dataframe(input_df):
            raise TypeError(
                "input_df must be a pyspark DataFrame (classic or Spark Connect), "
                f"got: {type(input_df).__module__}.{type(input_df).__name__}"
            )

        if isinstance(rules, str):
            rules = [rules]
        if not isinstance(rules, list) or not rules:
            raise ValueError(
                "rules must be a table_fqn string or a non-empty list of table_fqn strings"
            )

        normalized_rules = list(dict.fromkeys(
            r.strip() for r in rules if isinstance(r, str) and r.strip()
        ))
        if not normalized_rules:
            raise ValueError("rules must contain at least one non-empty table_fqn string")

        is_cross_table = len(normalized_rules) > 1
        # _data_source: the single source fqn, or a comma-joined list for a
        # cross-table run. Per-rule attribution is in _rule_sources either way.
        data_source = normalized_rules[0] if not is_cross_table else ",".join(normalized_rules)
        logger.info(
            f"Starting {'cross-table ' if is_cross_table else ''}validation. "
            f"Rule sources: {normalized_rules}"
        )

        # Rules are always read from self.rules_table (the env this Validator
        # was built for), filtered to these table_fqns. An unknown or
        # wrong-catalog fqn simply matches no rows and raises below, so no
        # extra cross-env guard is needed here. Quarantine then follows the
        # catalog named in each matched table_fqn.
        original_columns = list(input_df.columns)
        checks = self._load_checks_from_rules_table(normalized_rules)
        valid_df, invalid_df = self.dq_engine.apply_checks_by_metadata_and_split(input_df, checks)

        if not invalid_df.isEmpty():
            # Quarantine row = the input row as-is (all input columns) +
            # DQX's _error / _warning result columns + _row_id + audit columns.
            row_id_expr, row_id_keys = self._row_id_column(
                invalid_df, row_id_columns, original_columns
            )
            invalid_df = (
                invalid_df
                .withColumn(ROW_ID_COLUMN, row_id_expr)
                .withColumn(ROW_ID_KEYS_COLUMN, F.lit(row_id_keys))
                .withColumn(GENERATED_AT_COLUMN, F.current_timestamp())
                .withColumn("_data_source", F.lit(data_source))
                .withColumn("_rule_sources", F.lit(",".join(normalized_rules)))
                .withColumn("_is_cross_table", F.lit(is_cross_table))
            )

            target_quarantine_table = self._resolve_quarantine_table_name(normalized_rules)
            logger.info(f"Resolved quarantine table: {target_quarantine_table}")

            self._create_quarantine_if_missing(target_quarantine_table, invalid_df)

            (
                invalid_df.write
                .format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .saveAsTable(target_quarantine_table)
            )
            logger.info(f"Appended invalid records to: {target_quarantine_table}")
            invalid_count = invalid_df.count()
            logger.info(f"Invalid records count: {invalid_count}")
            print(f"Invalid records count: {invalid_count}")
        else:
            logger.info("No invalid records detected.")

        logger.info("Validation complete.")
        return valid_df


    def _normalize_column_names(self, column_names: list[str] | str) -> list[str]:
        """
        Normalize column_names input into a non-empty unique list preserving order.
        """
        if isinstance(column_names, str):
            normalized = [column_names.strip()] if column_names.strip() else []
        elif isinstance(column_names, list):
            normalized = [
                c.strip() for c in column_names
                if isinstance(c, str) and c.strip()
            ]
        else:
            raise TypeError("column_names must be a string or list of strings")

        normalized = list(dict.fromkeys(normalized))
        if not normalized:
            raise ValueError("column_names must contain at least one non-empty column name")
        return normalized


    def _bind_universal_check_to_columns(
        self,
        check_template: dict,
        column_name: str,
        all_columns: list[str],
    ) -> dict:
        """
        Bind a universal check template to specific column values.
        Supported placeholders in string fields:
          - {{column}}, ${column}, __COLUMN__
          - {{columns}}, ${columns}, __COLUMNS__ (replaced with CSV)
          - {{columns_csv}}, ${columns_csv}
        """

        column_tokens = {"{{column}}", "${column}", "__COLUMN__"}
        columns_tokens = {"{{columns}}", "${columns}", "__COLUMNS__"}

        def _replace(value):
            if isinstance(value, dict):
                return {k: _replace(v) for k, v in value.items()}
            if isinstance(value, list):
                return [_replace(v) for v in value]
            if isinstance(value, str):
                if value in columns_tokens:
                    return all_columns

                out = value
                for token in column_tokens:
                    out = out.replace(token, column_name)
                for token in columns_tokens:
                    out = out.replace(token, ",".join(all_columns))
                out = out.replace("{{columns_csv}}", ",".join(all_columns))
                out = out.replace("${columns_csv}", ",".join(all_columns))
                return out
            return value

        bound = _replace(copy.deepcopy(check_template))

        if isinstance(bound, dict):
            if "column" in bound and bound.get("column") in (None, "", "*", "column"):
                bound["column"] = column_name
            if "columns" in bound and bound.get("columns") in (None, [], "", "columns"):
                bound["columns"] = all_columns

        return bound

    def run_universal_rule_validation(
        self,
        input_df: DataFrame,
        rule_name: str,
        column_names: list[str] | str,
        source_table: str = None,
        row_id_columns: list[str] | str | None = None,
    ) -> DataFrame:
        """
        Run one approved universal rule (identified by rule_name -> rule_fqn)
        against one or more columns on the provided input_df.

        Universal rules are not bound to a table, so the quarantine location
        cannot be derived from the rule. Pass source_table=
        '<catalog>.<schema>.<table>' to say where bad rows should be
        quarantined; if omitted, it is inferred from input_df's query plan
        (best-effort, may fail on some runtimes).
        """
        if not _is_dataframe(input_df):
            raise TypeError(
                "input_df must be a pyspark DataFrame (classic or Spark Connect), "
                f"got: {type(input_df).__module__}.{type(input_df).__name__}"
            )
        if not isinstance(rule_name, str) or not rule_name.strip():
            raise ValueError("rule_name must be a non-empty string")
        if source_table is not None and (
            not isinstance(source_table, str) or len(source_table.split(".")) != 3
        ):
            raise ValueError(
                "source_table must be '<catalog>.<schema>.<table>', "
                f"got: '{source_table}'"
            )

        original_columns = list(input_df.columns)
        normalized_columns = self._normalize_column_names(column_names)
        missing_columns = [c for c in normalized_columns if c not in input_df.columns]
        if missing_columns:
            raise ValueError(
                f"Column(s) not found in input_df: {missing_columns}. "
                f"Available columns: {input_df.columns}"
            )

        normalized_rule_name = rule_name.strip()
        universal_rules_table = self.universal_rules_table
        logger.info(
            f"Loading approved universal rule '{normalized_rule_name}' "
            f"from {universal_rules_table}"
        )

        universal_df = (
            self.spark.read.table(universal_rules_table)
            .filter(F.lower(F.col("status")) == F.lit("approved"))
            .filter(F.lower(F.col("rule_fqn")) == F.lit(normalized_rule_name.lower()))
        )

        order_cols = [F.col("version").desc_nulls_last()]
        if "updated_at" in universal_df.columns:
            order_cols.append(F.col("updated_at").desc_nulls_last())

        universal_row = (
            universal_df
            .withColumn("__rn", F.row_number().over(Window.partitionBy("rule_fqn").orderBy(*order_cols)))
            .filter(F.col("__rn") == 1)
            .select("rule_id", "rule_fqn", "version", F.to_json(F.col("check")).alias("check_json"))
            .first()
        )

        if not universal_row:
            raise ValueError(
                f"No approved universal rule found for rule_name='{normalized_rule_name}' "
                f"in {universal_rules_table}"
            )

        check_json = universal_row["check_json"]
        if not check_json:
            raise ValueError(
                f"Universal rule '{normalized_rule_name}' has empty check payload"
            )

        try:
            parsed_check = json.loads(check_json)
        except Exception as e:
            raise ValueError(
                f"Universal rule '{normalized_rule_name}' has invalid check payload: {e}"
            ) from e

        check_templates = parsed_check if isinstance(parsed_check, list) else [parsed_check]
        bound_checks = []
        for col_name in normalized_columns:
            for template in check_templates:
                if not isinstance(template, dict):
                    raise ValueError(
                        f"Unsupported check payload type for rule '{normalized_rule_name}': "
                        f"{type(template).__name__}. Expected dict or list[dict]."
                    )
                bound_checks.append(
                    self._bind_universal_check_to_columns(
                        check_template=template,
                        column_name=col_name,
                        all_columns=normalized_columns,
                    )
                )

        logger.info(
            f"Applying universal rule '{normalized_rule_name}' "
            f"(rule_id={universal_row['rule_id']}, version={universal_row['version']}) "
            f"to columns: {normalized_columns}"
        )

        valid_df, invalid_df = self.dq_engine.apply_checks_by_metadata_and_split(input_df, bound_checks)

        if not invalid_df.isEmpty():
            source_fqn = source_table or self._infer_source_table_fqn(input_df)
            target_quarantine_table = self._resolve_quarantine_table_name(source_fqn)
            logger.info(f"Resolved quarantine table: {target_quarantine_table}")

            # Quarantine row = the input row as-is (all input columns) +
            # DQX's _error / _warning result columns + audit columns.
            row_id_expr, row_id_keys = self._row_id_column(
                invalid_df, row_id_columns, original_columns
            )
            invalid_df = (
                invalid_df
                .withColumn(ROW_ID_COLUMN, row_id_expr)
                .withColumn(ROW_ID_KEYS_COLUMN, F.lit(row_id_keys))
                .withColumn(GENERATED_AT_COLUMN, F.current_timestamp())
                .withColumn("_data_source", F.lit(source_fqn))
                .withColumn("_rule_name", F.lit(normalized_rule_name))
                .withColumn("_rule_columns", F.lit(",".join(normalized_columns)))
            )

            self._create_quarantine_if_missing(target_quarantine_table, invalid_df)
            (
                invalid_df.write
                .format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .saveAsTable(target_quarantine_table)
            )
            logger.info(f"Appended invalid records to: {target_quarantine_table}")
            invalid_count = invalid_df.count()
            logger.info(f"Invalid records count: {invalid_count}")
            print(f"Invalid records count: {invalid_count}")
        else:
            logger.info("No invalid records detected.")

        logger.info("Universal rule validation complete.")
        return valid_df
    
    
    