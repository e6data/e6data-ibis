from __future__ import annotations

import contextlib
import math
from copy import deepcopy

import sqlglot.expressions as sge
from sqlglot import transforms
from sqlglot.dialects import (
    TSQL,
    ClickHouse,
    Hive,
    MySQL,
    Oracle,
    Postgres,
    Snowflake,
    Spark,
    SQLite,
    Trino,
)
from sqlglot.dialects.dialect import (
    max_or_greatest,
    min_or_least,
    rename_func,
    unit_to_str,
    regexp_replace_sql,
    approx_count_distinct_sql,
    timestrtotime_sql,
    datestrtodate_sql
)
from sqlglot.helper import find_new_name, seq_get

ClickHouse.Generator.TRANSFORMS |= {
    sge.ArraySize: rename_func("length"),
    sge.ArraySort: rename_func("arraySort"),
    sge.LogicalAnd: rename_func("min"),
    sge.LogicalOr: rename_func("max"),
}


class DataFusion(Postgres):
    class Generator(Postgres.Generator):
        TRANSFORMS = Postgres.Generator.TRANSFORMS.copy() | {
            sge.Select: transforms.preprocess([transforms.eliminate_qualify]),
            sge.Pow: rename_func("pow"),
            sge.IsNan: rename_func("isnan"),
            sge.CurrentTimestamp: rename_func("now"),
            sge.CurrentDate: rename_func("today"),
            sge.Split: rename_func("string_to_array"),
            sge.Array: rename_func("make_array"),
            sge.ArrayContains: rename_func("array_has"),
            sge.ArraySize: rename_func("array_length"),
        }


class Druid(Postgres):
    class Generator(Postgres.Generator):
        TRANSFORMS = Postgres.Generator.TRANSFORMS.copy() | {
            sge.ApproxDistinct: rename_func("approx_count_distinct"),
            sge.Pow: rename_func("power"),
        }


def _interval(self, e, quote_arg=True):
    """Work around the inability to handle string literals in INTERVAL syntax."""
    arg = e.args["this"].this
    with contextlib.suppress(AttributeError):
        arg = arg.sql(self.dialect)

    if quote_arg:
        arg = f"'{arg}'"

    return f"INTERVAL {arg} {e.args['unit']}"


class Exasol(Postgres):
    class Generator(Postgres.Generator):
        TRANSFORMS = Postgres.Generator.TRANSFORMS.copy() | {sge.Interval: _interval}
        TYPE_MAPPING = Postgres.Generator.TYPE_MAPPING.copy() | {
            sge.DataType.Type.TIMESTAMPTZ: "TIMESTAMP WITH LOCAL TIME ZONE",
        }


def _calculate_precision(interval_value: int) -> int:
    """Calculate interval precision.

    FlinkSQL interval data types use leading precision and fractional-
    seconds precision. Because the leading precision defaults to 2, we need to
    specify a different precision when the value exceeds 2 digits.

    (see
    https://learn.microsoft.com/en-us/sql/odbc/reference/appendixes/interval-literals)
    """
    # log10(interval_value) + 1 is equivalent to len(str(interval_value)), but is significantly
    # faster and more memory-efficient
    if interval_value == 0:
        return 0
    if interval_value < 0:
        raise ValueError(
            f"Expecting value to be a non-negative integer, got {interval_value}"
        )
    return int(math.log10(interval_value)) + 1


def _interval_with_precision(self, e):
    """Format interval with precision."""
    arg = e.args["this"].this
    formatted_arg = arg
    with contextlib.suppress(AttributeError):
        formatted_arg = arg.sql(self.dialect)

    unit = e.args["unit"]
    # when formatting interval scalars, need to quote arg and add precision
    if isinstance(arg, str):
        formatted_arg = f"'{formatted_arg}'"
        prec = _calculate_precision(int(arg))
        prec = max(prec, 2)
        unit.args["this"] += f"({prec})"

    return f"INTERVAL {formatted_arg} {unit}"


def _explode_to_unnest():
    """Convert explode into unnest.

    NOTE: Flink doesn't support UNNEST WITH ORDINALITY or UNNEST WITH OFFSET.
    """

    def _explode_to_unnest(expression: sge.Expression) -> sge.Expression:
        if isinstance(expression, sge.Select):
            from sqlglot.optimizer.scope import Scope

            taken_select_names = set(expression.named_selects)
            taken_source_names = {name for name, _ in Scope(expression).references}

            def new_name(names: set[str], name: str) -> str:
                name = find_new_name(names, name)
                names.add(name)
                return name

            # we use list here because expression.selects is mutated inside the loop
            for select in list(expression.selects):
                explode = select.find(sge.Explode)

                if explode:
                    explode_alias = ""

                    if isinstance(select, sge.Alias):
                        explode_alias = select.args["alias"]
                        alias = select
                    elif isinstance(select, sge.Aliases):
                        explode_alias = select.aliases[1]
                        alias = select.replace(sge.alias_(select.this, "", copy=False))
                    else:
                        alias = select.replace(sge.alias_(select, ""))
                        explode = alias.find(sge.Explode)
                        assert explode

                    explode_arg = explode.this

                    # This ensures that we won't use EXPLODE's argument as a new selection
                    if isinstance(explode_arg, sge.Column):
                        taken_select_names.add(explode_arg.output_name)

                    unnest_source_alias = new_name(taken_source_names, "_u")

                    if not explode_alias:
                        explode_alias = new_name(taken_select_names, "col")

                    alias.set("alias", sge.to_identifier(explode_alias))

                    column = sge.column(explode_alias, table=unnest_source_alias)

                    explode.replace(column)

                    expression.join(
                        sge.alias_(
                            sge.Unnest(
                                expressions=[explode_arg.copy()],
                            ),
                            unnest_source_alias,
                            table=[explode_alias],
                        ),
                        join_type="CROSS",
                        copy=False,
                    )

        return expression

    return _explode_to_unnest


class Flink(Hive):
    UNESCAPED_SEQUENCES = {"\\\\d": "\\d"}

    class Generator(Hive.Generator):
        UNNEST_WITH_ORDINALITY = False

        TYPE_MAPPING = Hive.Generator.TYPE_MAPPING.copy() | {
            sge.DataType.Type.TIME: "TIME",
            sge.DataType.Type.STRUCT: "ROW",
        }

        TRANSFORMS = Hive.Generator.TRANSFORMS.copy() | {
            sge.Select: transforms.preprocess([_explode_to_unnest()]),
            sge.Stddev: rename_func("stddev_samp"),
            sge.StddevPop: rename_func("stddev_pop"),
            sge.StddevSamp: rename_func("stddev_samp"),
            sge.Variance: rename_func("var_samp"),
            sge.VariancePop: rename_func("var_pop"),
            sge.ArrayConcat: rename_func("array_concat"),
            sge.ArraySize: rename_func("cardinality"),
            sge.Length: rename_func("char_length"),
            sge.TryCast: lambda self,
                                e: f"TRY_CAST({e.this.sql(self.dialect)} AS {e.to.sql(self.dialect)})",
            sge.DayOfYear: rename_func("dayofyear"),
            sge.DayOfWeek: rename_func("dayofweek"),
            sge.DayOfMonth: rename_func("dayofmonth"),
            sge.Interval: _interval_with_precision,
        }

        def struct_sql(self, expression: sge.Struct) -> str:
            from sqlglot.optimizer.annotate_types import annotate_types

            expression = annotate_types(expression)

            values = []
            schema = []

            for e in expression.expressions:
                if isinstance(e, sge.PropertyEQ):
                    e = sge.alias_(e.expression, e.this)
                # named structs
                if isinstance(e, sge.Alias):
                    if e.type and e.type.is_type(sge.DataType.Type.UNKNOWN):
                        self.unsupported(
                            "Cannot convert untyped key-value definitions (try annotate_types)."
                        )
                    else:
                        schema.append(f"{self.sql(e, 'alias')} {self.sql(e.type)}")
                    values.append(self.sql(e, "this"))
                else:
                    values.append(self.sql(e))

            if not (size := len(expression.expressions)) or len(schema) != size:
                return self.func("ROW", *values)
            return f"CAST(ROW({', '.join(values)}) AS ROW({', '.join(schema)}))"

        def array_sql(self, expression: sge.Array) -> str:
            # workaround for the time being because you cannot construct an array of named
            # STRUCTs directly from the ARRAY[] constructor
            # https://issues.apache.org/jira/browse/FLINK-34898
            from sqlglot.optimizer.annotate_types import annotate_types

            expression = annotate_types(expression)
            first_arg = seq_get(expression.expressions, 0)
            # it's an array of structs
            if isinstance(first_arg, sge.Struct):
                # get rid of aliasing because we want to compile this as CAST instead
                args = deepcopy(expression.expressions)
                for arg in args:
                    for e in arg.expressions:
                        arg.set("expressions", [e.unalias() for e in arg.expressions])

                format_values = ", ".join([self.sql(arg) for arg in args])
                # all elements of the array should have the same type
                format_dtypes = self.sql(first_arg.type)

                return f"CAST(ARRAY[{format_values}] AS ARRAY<{format_dtypes}>)"

            return (
                f"ARRAY[{', '.join(self.sql(arg) for arg in expression.expressions)}]"
            )

    class Tokenizer(Hive.Tokenizer):
        # In Flink, embedded single quotes are escaped like most other SQL
        # dialects: doubling up the single quote
        #
        # We override it here because we inherit from Hive's dialect and Hive
        # uses a backslash to escape single quotes
        STRING_ESCAPES = ["'"]


class Impala(Hive):
    NULL_ORDERING = "nulls_are_large"

    class Generator(Hive.Generator):
        TRANSFORMS = Hive.Generator.TRANSFORMS.copy() | {
            sge.ApproxDistinct: rename_func("ndv"),
            sge.IsNan: rename_func("is_nan"),
            sge.IsInf: rename_func("is_inf"),
            sge.DayOfWeek: rename_func("dayofweek"),
            sge.Interval: lambda self, e: _interval(self, e, quote_arg=False),
            sge.CurrentDate: rename_func("current_date"),
        }


class MSSQL(TSQL):
    class Generator(TSQL.Generator):
        TRANSFORMS = TSQL.Generator.TRANSFORMS.copy() | {
            sge.ApproxDistinct: rename_func("approx_count_distinct"),
            sge.Stddev: rename_func("stdevp"),
            sge.StddevPop: rename_func("stdevp"),
            sge.StddevSamp: rename_func("stdev"),
            sge.Variance: rename_func("var"),
            sge.VariancePop: rename_func("varp"),
            sge.Ceil: rename_func("ceiling"),
            sge.Trim: lambda self, e: f"TRIM({e.this.sql(self.dialect)})",
            sge.DateFromParts: rename_func("datefromparts"),
        }


MySQL.Generator.TRANSFORMS |= {
    sge.LogicalOr: rename_func("max"),
    sge.LogicalAnd: rename_func("min"),
    sge.VariancePop: rename_func("var_pop"),
    sge.Variance: rename_func("var_samp"),
    sge.Stddev: rename_func("stddev_pop"),
    sge.StddevPop: rename_func("stddev_pop"),
    sge.StddevSamp: rename_func("stddev_samp"),
    sge.RegexpLike: (
        lambda _, e: f"({e.this.sql('mysql')} RLIKE {e.expression.sql('mysql')})"
    ),
}


def _create_sql(self, expression: sge.Create) -> str:
    properties = expression.args.get("properties")
    temporary = any(
        isinstance(prop, sge.TemporaryProperty)
        for prop in (properties.expressions if properties else [])
    )
    kind = expression.args["kind"]
    if kind.upper() in ("TABLE", "VIEW") and temporary:
        # Force insertion of required "GLOBAL" keyword
        expression_sql = self.create_sql(expression).replace(
            "CREATE TEMPORARY", "CREATE GLOBAL TEMPORARY"
        )
        if expression.expression:  #  CREATE ... AS ...
            return self.sql(expression_sql, "expression")
        else:  #  CREATE ... ON COMMIT PRESERVE ROWS
            # Autocommit does not work here for some reason so we append it manually
            return self.sql(
                expression_sql + " ON COMMIT PRESERVE ROWS",
                "expression",
            )
    return self.create_sql(expression)


# hack around https://github.com/tobymao/sqlglot/issues/3684
Oracle.NULL_ORDERING = "nulls_are_large"
Oracle.Generator.TRANSFORMS |= {
    sge.LogicalOr: rename_func("max"),
    sge.LogicalAnd: rename_func("min"),
    sge.VariancePop: rename_func("var_pop"),
    sge.Variance: rename_func("var_samp"),
    sge.Stddev: rename_func("stddev_pop"),
    sge.ApproxDistinct: rename_func("approx_count_distinct"),
    sge.Create: _create_sql,
    sge.Select: transforms.preprocess([transforms.eliminate_semi_and_anti_joins]),
}

# TODO: can delete this after bumping sqlglot version > 20.9.0
Oracle.Generator.TYPE_MAPPING |= {
    sge.DataType.Type.TIMETZ: "TIME",
    sge.DataType.Type.TIMESTAMPTZ: "TIMESTAMP",
}
Oracle.Generator.TZ_TO_WITH_TIME_ZONE = True


class Polars(Postgres):
    """Subclass of Postgres dialect for Polars.

    This is here to allow referring to the Postgres dialect as "polars"
    """


Postgres.Generator.TRANSFORMS |= {
    sge.Map: rename_func("hstore"),
    sge.Split: rename_func("string_to_array"),
    sge.RegexpSplit: rename_func("regexp_split_to_array"),
    sge.DateFromParts: rename_func("make_date"),
    sge.ArraySize: rename_func("cardinality"),
    sge.Pow: rename_func("pow"),
}


class PySpark(Spark):
    """Subclass of Spark dialect for PySpark.

    This is here to allow referring to the Spark dialect as "pyspark"
    """


class RisingWave(Postgres):
    # Need to disable timestamp precision
    # No "or replace" allowed in create statements
    # no "not null" clause for column constraints

    class Generator(Postgres.Generator):
        SINGLE_STRING_INTERVAL = True
        RENAME_TABLE_WITH_DB = False
        LOCKING_READS_SUPPORTED = True
        JOIN_HINTS = False
        TABLE_HINTS = False
        QUERY_HINTS = False
        NVL2_SUPPORTED = False
        PARAMETER_TOKEN = "$"  # noqa: S105
        TABLESAMPLE_SIZE_IS_ROWS = False
        TABLESAMPLE_SEED_KEYWORD = "REPEATABLE"
        SUPPORTS_SELECT_INTO = True
        JSON_TYPE_REQUIRED_FOR_EXTRACTION = True
        SUPPORTS_UNLOGGED_TABLES = True

        TYPE_MAPPING = Postgres.Generator.TYPE_MAPPING.copy() | {
            sge.DataType.Type.TIMESTAMPTZ: "TIMESTAMPTZ"
        }


Snowflake.Generator.TRANSFORMS |= {
    sge.ApproxDistinct: rename_func("approx_count_distinct"),
    sge.Levenshtein: rename_func("editdistance"),
}

SQLite.Generator.TYPE_MAPPING |= {sge.DataType.Type.BOOLEAN: "BOOLEAN"}


# TODO(cpcloud): remove this hack once
# https://github.com/tobymao/sqlglot/issues/2735 is resolved
def make_cross_joins_explicit(node):
    if not (node.kind or node.side):
        node.args["kind"] = "CROSS"
    return node


Trino.Generator.TRANSFORMS |= {
    sge.BitwiseLeftShift: rename_func("bitwise_left_shift"),
    sge.BitwiseRightShift: rename_func("bitwise_right_shift"),
    sge.FirstValue: rename_func("first_value"),
    sge.Join: transforms.preprocess([make_cross_joins_explicit]),
    sge.LastValue: rename_func("last_value"),
}


class E6data(MySQL):
    class Tokenizer(MySQL.Tokenizer):
        IDENTIFIERS = ['"']
    class Generator(MySQL.Generator):
        def unnest_sql(self, expression: sge.Explode) -> str:
            # Extract array expressions
            array_expr = expression.args.get("expressions")

            # Format array expressions to SQL
            if isinstance(array_expr, list):
                array_expr_sql = ', '.join(self.sql(arg) for arg in array_expr)
            else:
                array_expr_sql = self.sql(array_expr)

            # Process the alias
            alias = self.sql(expression, "alias")

            # Handle the columns for alias arguments (e.g., t(x))
            alias_args = expression.args.get("alias")
            alias_columns = ""

            if alias_args and alias_args.args.get("columns"):
                # Extract the columns for alias arguments
                alias_columns_list = [self.sql(col) for col in alias_args.args["columns"]]
                alias_columns = f"({', '.join(alias_columns_list)})"

            # Construct the alias string
            alias_sql = f" AS {alias}{alias_columns}" if alias else ""

            # Generate the final UNNEST SQL
            return f"UNNEST({array_expr_sql}){alias_sql}"

        def extract_sql(self, expression: sge.Extract) -> str:
            unit = expression.this.name
            expression_sql = self.sql(expression, "expression")
            extract_str = f"EXTRACT({unit} FROM {expression_sql})"
            return extract_str

        def generateseries_sql(self, expression: sge.GenerateSeries) -> str:
            start = expression.args["start"]
            end = expression.args["end"]
            step = expression.args.get("step")

            return self.func("SEQUENCE", start, end, step)

        def _last_day_sql(self, expression: sge.LastDay) -> str:
            # date_expr = self.sql(expression,"this")
            date_expr = expression.args.get("this")
            date_expr = date_expr
            if isinstance(date_expr, sge.Literal):
                date_expr = f"CAST({date_expr} AS DATE)"
            return f"LAST_DAY({date_expr})"

        def regexp_replace_sql(self, expression: sge.RegexpReplace) -> str:
            bad_args = list(filter(expression.args.get, ("position", "occurrence", "modifiers")))
            if bad_args:
                self.unsupported(f"REGEXP_REPLACE does not support the following arg(s): {bad_args}")
            if not "replacement" in expression.args.keys():
                replacement = ''
            else:
                replacement = expression.args["replacement"]

            return self.func(
                "REGEXP_REPLACE", expression.this, expression.expression, replacement
            )

        def _from_unixtime_withunit_sql(self, expression: sge.UnixToTime | sge.UnixToStr) -> str:
            seconds_str = f"'seconds'"
            milliseconds_str = f"'milliseconds'"
            if isinstance(expression, sge.UnixToTime):
                timestamp = self.sql(expression, "this")
                # scale = expression.args.get("scale")
                # # this by default value for seconds is been kept for now
                # if scale is None:
                #     scale = 'seconds'
                scale = expression.args.get("scale",
                                            sge.Literal.string('seconds'))  # Default to 'seconds' if scale is None

                # Extract scale string, ensure it is lowercase and strip any extraneous quotes
                scale_str = self.sql(scale).lower().strip('"').strip("'")
                if scale_str == 'seconds':
                    return self.func("FROM_UNIXTIME_WITHUNIT", timestamp, seconds_str)
                elif scale_str == 'milliseconds':
                    return self.func("FROM_UNIXTIME_WITHUNIT", timestamp, milliseconds_str)
                else:
                    raise ValueError(
                        f"Unsupported unit for FROM_UNIXTIME_WITHUNIT: {scale_str} and we only support 'seconds' and 'milliseconds'")
            else:
                timestamp = self.sql(expression, "this")
                if isinstance(expression.this, sge.Div) and (expression.this.right.this == '1000'):
                    return self.func("FROM_UNIXTIME_WITHUNIT", timestamp, seconds_str)
                return self.func("FROM_UNIXTIME_WITHUNIT", timestamp, milliseconds_str)

        def _to_unix_timestamp_sql(self, expression: sge.TimeToUnix | sge.StrToUnix) -> str:
            timestamp = self.sql(expression, "this")
            # if not (isinstance(timestamp, sge.Cast) and timestamp.to.is_type(sge.DataType.Type.TIMESTAMP)):
            # if isinstance(timestamp, (sge.Literal, sge.Column)):
            #     timestamp = f"CAST({timestamp} AS TIMESTAMP)"
            return self.func("TO_UNIX_TIMESTAMP", timestamp)

        def tochar_sql(self, expression: sge.ToChar) -> str:
            date_expr = expression.this
            if (isinstance(date_expr, sge.Cast) and not (date_expr.to.this.name == 'TIMESTAMP')) or (
                    not isinstance(date_expr, sge.Cast) and
                    not sge.DataType.is_type(date_expr, sge.DataType.Type.DATE) or sge.DataType.is_type(date_expr,
                                                                                                        sge.DataType.Type.TIMESTAMP)):
                date_expr = f"CAST({date_expr} AS TIMESTAMP)"
            format_expr = self.format_time(expression)
            return f"TO_CHAR({date_expr},'{format_expr}')"

        def date_trunc_sql(self, expression: sge.DateTrunc | sge.TimestampTrunc):
            unit = unit_to_str(expression.unit)
            date = expression.this
            return self.func("DATE_TRUNC", unit, date)

        def filter_array_sql(self, expression: sge.ArrayFilter) -> str:
            cond = expression.expression
            if isinstance(cond, sge.Lambda) and len(cond.expressions) == 1:
                alias = cond.expressions[0]
                cond = cond.this
            elif isinstance(cond, sge.Predicate):
                alias = "_u"
            else:
                self.unsupported("Unsupported filter condition")
                return ""

            # Check for aggregate functions
            if any(isinstance(node, sge.AggFunc) for node in cond.find_all(sge.Expression)):
                raise ValueError("array filter's Lambda expression are not supported with aggregate functions")
                return ""

            lambda_expr = f"{alias} -> {self.sql(cond)}"
            return f"FILTER_ARRAY({self.sql(expression.this)}, {lambda_expr})"

        def bracket_sql(self, expression: sge.Bracket) -> str:
            return self.func(
                "ELEMENT_AT",
                expression.this,
                seq_get(
                    apply_index_offset(
                        expression.this,
                        expression.expressions,
                        1 - expression.args.get("offset", 0),
                    ),
                    0,
                ),
            )

        def format_date_sql(self, expression: sge.TimeToStr) -> str:
            date_expr = expression.this
            format_expr = self.format_time(expression)
            format_expr_quoted = f"'{format_expr}'"
            if isinstance(date_expr, sge.CurrentDate) or isinstance(date_expr, sge.CurrentTimestamp) or isinstance(
                    date_expr, sge.TsOrDsToDate):
                return self.func("FORMAT_DATE", date_expr, format_expr_quoted)
            if isinstance(date_expr, sge.Cast) and not (
                    date_expr.to.this.name == 'TIMESTAMP' or date_expr.to.this.name == 'DATE'):
                date_expr = f"CAST({date_expr} AS DATE)"
            return self.func("FORMAT_DATE", date_expr, format_expr_quoted)

        def interval_sql(self, expression: sge.Interval) -> str:
            if expression.this and expression.unit:
                value = expression.this.name
                unit = expression.unit.name
                interval_str = f"INTERVAL {value} {unit}"
                return interval_str
            else:
                return ""

        UNSIGNED_TYPE_MAPPING = {
            sge.DataType.Type.UBIGINT: "BIGINT",
            sge.DataType.Type.UINT: "INT",
            sge.DataType.Type.UMEDIUMINT: "INT",
            sge.DataType.Type.USMALLINT: "INT",
            sge.DataType.Type.UTINYINT: "INT",
            sge.DataType.Type.UDECIMAL: "DECIMAL",
        }

        CAST_SUPPORTED_TYPE_MAPPING = {
            sge.DataType.Type.NCHAR: "CHAR",
            sge.DataType.Type.VARCHAR: "VARCHAR",
            sge.DataType.Type.INT: "INT",
            sge.DataType.Type.TINYINT: "INT",
            sge.DataType.Type.SMALLINT: "INT",
            sge.DataType.Type.MEDIUMINT: "INT",
            sge.DataType.Type.BIGINT: "BIGINT",
            sge.DataType.Type.BOOLEAN: "BOOLEAN",
            sge.DataType.Type.DATE: "DATE",
            sge.DataType.Type.DATE32: "DATE",
            sge.DataType.Type.FLOAT: "FLOAT",
            sge.DataType.Type.DOUBLE: "DOUBLE",
            sge.DataType.Type.TIMESTAMP: "TIMESTAMP",
            sge.DataType.Type.TIMESTAMPTZ: "TIMESTAMP",
            sge.DataType.Type.TIMESTAMPNTZ: "TIMESTAMP",
            sge.DataType.Type.TEXT: "VARCHAR",
            sge.DataType.Type.TINYTEXT: "VARCHAR",
            sge.DataType.Type.MEDIUMTEXT: "VARCHAR",
            sge.DataType.Type.DECIMAL: "DECIMAL"
        }

        TYPE_MAPPING = {
            **UNSIGNED_TYPE_MAPPING,
            **CAST_SUPPORTED_TYPE_MAPPING,
            sge.DataType.Type.JSON: "JSON",
            sge.DataType.Type.STRUCT: "STRUCT",
            sge.DataType.Type.ARRAY: "ARRAY"
        }

        TRANSFORMS = {
            # sge.Concat: lambda self, e: f"concat({self.sql(e.left)}, {self.sql(e.right)})",
            **generator.Generator.TRANSFORMS,
            sge.AnyValue: rename_func("ARBITRARY"),
            sge.ApproxDistinct: approx_count_distinct_sql,
            sge.ApproxQuantile: rename_func("APPROX_PERCENTILE"),
            sge.ArgMax: rename_func("MAX_BY"),
            sge.ArrayAgg: rename_func("COLLECT_LIST"),
            sge.ArrayConcat: rename_func("ARRAY_CONCAT"),
            sge.ArrayContains: rename_func("ARRAY_CONTAINS"),
            sge.ArrayFilter: filter_array_sql,
            sge.ArrayToString: rename_func("ARRAY_JOIN"),
            sge.ArraySize: rename_func("size"),
            sge.AtTimeZone: lambda self, e: self.func(
                "DATETIME", e.this, e.args.get("zone")
            ),
            sge.BitwiseLeftShift: lambda self, e: self.func("SHIFTLEFT", e.this, e.expression),
            sge.BitwiseNot: lambda self, e: self.func("BITWISE_NOT", e.this),
            sge.BitwiseOr: lambda self, e: self.func("BITWISE_OR", e.this, e.expression),
            sge.BitwiseRightShift: lambda self, e: self.func("SHIFTRIGHT", e.this, e.expression),
            sge.BitwiseXor: lambda self, e: self.func("BITWISE_XOR", e.this, e.expression),
            sge.Bracket: bracket_sql,
            sge.CurrentDate: lambda *_: "CURRENT_DATE",
            sge.CurrentTimestamp: lambda *_: "CURRENT_TIMESTAMP",
            sge.Date: lambda self, e: self.func("DATE", e.this),
            sge.DateAdd: lambda self, e: self.func(
                "DATE_ADD",
                unit_to_str(e),
                _to_int(e.expression),
                e.this,
            ),
            sge.DateDiff: lambda self, e: self.func(
                "DATE_DIFF",
                unit_to_str(e),
                e.expression,
                e.this,
            ),
            sge.DateTrunc: date_trunc_sql,
            sge.Explode: unnest_sql,
            sge.Extract: extract_sql,
            sge.FirstValue: rename_func("FIRST_VALUE"),
            sge.FromTimeZone: lambda self, e: self.func(
                "CONVERT_TIMEZONE", "'UTC'", e.args.get("zone"), e.this
            ),
            sge.GenerateSeries: generateseries_sql,
            sge.GroupConcat: lambda self, e: self.func(
                "LISTAGG" if e.args.get("within_group") else "STRING_AGG",
                e.this,
                e.args.get("separator") or sge.Literal.string(',')
            ),
            sge.Interval: interval_sql,
            sge.JSONExtract: lambda self, e: self.func("json_extract", e.this, e.expression),
            sge.JSONExtractScalar: lambda self, e: self.func("json_extract", e.this, e.expression),
            sge.Lag: lambda self, e: self.func("LAG", e.this, e.args.get("offset")),
            sge.LastDay: _last_day_sql,
            sge.LastValue: rename_func("LAST_VALUE"),
            sge.Lead: lambda self, e: self.func("LEAD", e.this, e.args.get("offset")),
            sge.Length: rename_func("LENGTH"),
            sge.Log: rename_func("LN"),
            sge.Max: max_or_greatest,
            sge.MD5Digest: lambda self, e: self.func("MD5", e.this),
            sge.Min: min_or_least,
            sge.Mod: lambda self, e: self.func("MOD", e.this, e.expression),
            sge.Nullif: rename_func("NULLIF"),
            sge.Pow: rename_func("POWER"),
            sge.RegexpExtract: rename_func("REGEXP_EXTRACT"),
            sge.RegexpLike: lambda self, e: self.func("REGEXP_LIKE", e.this, e.expression),
            sge.RegexpReplace: regexp_replace_sql,
            sge.RegexpSplit: rename_func("SPLIT_PART"),
            # sge.Select: select_sql,
            sge.Split: rename_func("SPLIT"),
            sge.Stddev: rename_func("STDDEV"),
            sge.StddevPop: rename_func("STDDEV_POP"),
            sge.StrPosition: lambda self, e: self.func(
                "LOCATE", e.args.get("substr"), e.this, e.args.get("position")
            ),
            sge.StrToDate: lambda self, e: self.func("TO_DATE", e.this, self.format_time(e)),
            sge.StrToTime: lambda self, e: self.func("TO_TIMESTAMP", e.this, self.format_time(e)),
            sge.StrToUnix: _to_unix_timestamp_sql,
            sge.StartsWith: rename_func("STARTS_WITH"),
            # sge.Struct: struct_sql,
            sge.TimeToStr: format_date_sql,
            sge.TimeStrToTime: timestrtotime_sql,
            sge.TimeStrToDate: datestrtodate_sql,
            sge.TimeToUnix: _to_unix_timestamp_sql,
            sge.Timestamp: lambda self, e: self.func("TIMESTAMP", e.this),
            sge.TimestampAdd: lambda self, e: self.func(
                "TIMESTAMP_ADD", unit_to_str(e), e.expression, e.this
            ),
            sge.TimestampDiff: lambda self, e: self.func(
                "TIMESTAMP_DIFF",
                unit_to_str(e),
                e.expression,
                e.this,
            ),
            sge.TimestampTrunc: date_trunc_sql,
            sge.ToChar: tochar_sql,
            sge.Trim: lambda self, e: self.func("TRIM", e.this, ' '),
            sge.TsOrDsAdd: lambda self, e: self.func(
                "DATE_ADD",
                unit_to_str(e),
                _to_int(e.expression),
                e.this,
            ),
            sge.TsOrDsDiff: lambda self, e: self.func(
                "DATE_DIFF",
                unit_to_str(e),
                e.expression,
                e.this,
            ),
            sge.UnixToTime: _from_unixtime_withunit_sql,
            sge.UnixToStr: _from_unixtime_withunit_sql
        }
