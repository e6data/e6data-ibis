"""The E6Data backend."""

from __future__ import annotations

import contextlib
import re
import warnings
from functools import cached_property, partial
from itertools import repeat
from operator import itemgetter
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qs, urlparse

import numpy as np
import sqlglot as sg
import sqlglot.expressions as sge

import ibis
import ibis.common.exceptions as com
import ibis.expr.operations as ops
import ibis.expr.schema as sch
import ibis.expr.types as ir
from ibis import util
from ibis.backends import CanCreateDatabase
from ibis.backends.e6data.compiler import E6DataCompiler
from ibis.backends.mysql import Backend as MySQLBackend
from ibis.backends.sql.compiler import TRUE, C
from e6data_python_connector import Connection

if TYPE_CHECKING:
    from collections.abc import Mapping

    import pandas as pd
    import polars as pl
    import pyarrow as pa


class Backend(MySQLBackend, CanCreateDatabase):
    name = "e6data"
    compiler = E6DataCompiler()
    supports_create_or_replace = False

    @cached_property
    def version(self):
        matched = re.search(r"(\d+)\.(\d+)\.(\d+)", self.con.server_version)
        return ".".join(matched.groups())

    def _from_url(self, url: str, **kwargs):
        """Connect to a backend using a URL `url`.

        Parameters
        ----------
        url
            URL with which to connect to a backend.
        kwargs
            Additional keyword arguments

        Returns
        -------
        BaseBackend
            A backend instance

        """

        url = urlparse(url)
        database, *_ = url.path[1:].split("/", 1)
        query_params = parse_qs(url.query)
        connect_args = {
            "user": url.username,
            "password": url.password or "",
            "host": url.hostname,
            "database": database or "",
            "catalog_name": url.catalog or "",
            "secure": url.secure == 'true',
            "auto_resume": query_params.get('auto-resume', [False])[0] == 'true',
            "cluster_uuid":  query_params.get('cluster-uuid', [None])[0] or "",
        }

        for name, value in query_params.items():
            if len(value) > 1:
                connect_args[name] = value
            elif len(value) == 1:
                connect_args[name] = value[0]
            else:
                raise com.IbisError(f"Invalid URL parameter: {name}")

        kwargs.update(connect_args)
        self._convert_kwargs(kwargs)

        if "user" in kwargs and not kwargs["user"]:
            del kwargs["user"]

        if "host" in kwargs and not kwargs["host"]:
            del kwargs["host"]

        if "database" in kwargs and not kwargs["database"]:
            del kwargs["database"]

        if "password" in kwargs and kwargs["password"] is None:
            del kwargs["password"]

        if "catalog_name" in kwargs and not kwargs["catalog_name"]:
            del kwargs["catalog_name"]

        return self.connect(**kwargs)

    @cached_property
    def version(self):
        matched = re.search(r"(\d+)\.(\d+)\.(\d+)", self.con.server_version)
        return ".".join(matched.groups())

    def do_connect(
        self,
        host: str,
        port: int,
        username: str,
        password: str,
        database: str,
        catalog_name: str,
        **kwargs,
    ) -> None:
        """Create an Ibis client using the passed connection parameters.

          host
            Hostname
          port
            Port
          username
            Username
          password
            Password
          database
            Database to connect to
          catalog_name
          Catalog name
          kwargs
            Additional keyword arguments


        Examples
        --------
        >>> import os
        >>> import getpass
        >>> host = os.environ.get("IBIS_TEST_E6DATA_HOST", "localhost")
        >>> user = os.environ.get("IBIS_TEST_E6DATA_USER", getpass.getuser())
        >>> password = os.environ.get("IBIS_TEST_E6DATA_PASSWORD")
        >>> database = os.environ.get("IBIS_TEST_E6DATA_DATABASE", "ibis_testing")
        >>> con = connect(database=database, host=host, user=user, password=password)
        >>> con.list_tables()  # doctest: +ELLIPSIS
        [...]
        >>> t = con.table("functional_alltypes")
        >>> t

        """
        self.con = Connection(
            host=host,
            port=port,
            username=username,
            password=password,
            database=database,
            catalog=catalog_name,
        )

    @property
    def current_database(self) -> str:
        with self._safe_raw_sql(sg.select(self.compiler.f.database())) as cur:
            [(database,)] = cur.fetchall()
        return database

    def list_databases(self, like: str | None = None) -> list[str]:
        # In MySQL syntax, "database" and "schema" are synonymous

        databases = self.con.get_schema_names()
        return self._filter_with_like(databases, like)

    def _get_schema_using_query(self, query: str) -> sch.Schema:
        table = util.gen_name(f"{self.name}_metadata")

        with self.begin() as cur:
            cur.execute(
                f"CREATE TEMPORARY TABLE {table} AS SELECT * FROM ({query}) AS tmp LIMIT 0"
            )
            try:
                return self.get_schema(table)
            finally:
                cur.execute(f"DROP TABLE {table}")

    def table(
        self,
        name: str,
        database: tuple[str, str] | str | None = None
    ) -> ir.Table:
        """Construct a table expression.

        Parameters
        ----------
        name : str
            Table name
        database : tuple[str, str] | str | None, optional
            Database name. If not provided, the current database is used.
            For backends that support multi-level table hierarchies, you can
            pass in a dotted string path like "catalog.database" or a tuple of
            strings like ("catalog", "database").

        Returns
        -------
        ir.Table
            Table expression
        """
        catalog = None
        db = None

        if isinstance(database, tuple):
            catalog, db = database
        elif isinstance(database, str):
            if '.' in database:
                catalog, db = database.split('.', 1)
            else:
                db = database

        table_schema = self.get_schema(name, catalog=catalog, database=db)

        return ops.DatabaseTable(
            name,
            schema=table_schema,
            source=self,
            namespace=ops.Namespace(catalog=catalog, database=db),
        ).to_expr()

    def get_schema(
        self, name: str, *, catalog: str | None = None, database: str | None = None
    ) -> sch.Schema:
        # print db, catalog, table
        columns = self.con.get_columns(
            database=database, catalog=catalog, table=name)
        type_mapper = self.compiler.type_mapper
        fields = {
            column["fieldName"]: type_mapper.from_string(
                column["fieldType"], nullable=True)
            for column in columns
        }
        return sch.Schema(fields)

    # TODO(kszucs): should make it an abstract method or remove the use of it
    # from .execute()
    @contextlib.contextmanager
    def _safe_raw_sql(self, *args, **kwargs):
        with contextlib.closing(self.raw_sql(*args, **kwargs)) as result:
            yield result

    def raw_sql(self, query: str | sg.Expression, **kwargs: Any) -> Any:
        with contextlib.suppress(AttributeError):
            query = query.sql(dialect=self.name)

        con = self.con
        cursor = con.cursor()

        try:
            cursor.execute(query, **kwargs)
        except Exception:
            cursor.close()
            raise
        else:
            con.commit()
            return cursor

    # TODO: disable positional arguments
    def list_tables(
        self,
        like: str | None = None,
        schema: str | None = None,
        database: tuple[str, str] | str | None = None,
    ) -> list[str]:
        """List the tables in the database.

        ::: {.callout-note}
        ## Ibis does not use the word `schema` to refer to database hierarchy.

        A collection of tables is referred to as a `database`.
        A collection of `database` is referred to as a `catalog`.

        These terms are mapped onto the corresponding features in each
        backend (where available), regardless of whether the backend itself
        uses the same terminology.
        :::

        Parameters
        ----------
        like
            A pattern to use for listing tables.
        schema
            [deprecated] The schema to perform the list against.
        database
            Database to list tables from. Default behavior is to show tables in
            the current database (``self.current_database``).
        """
        if schema is not None:
            self._warn_schema()

        if schema is not None and database is not None:
            raise ValueError(
                "Using both the `schema` and `database` kwargs is not supported. "
                "`schema` is deprecated and will be removed in Ibis 10.0"
                "\nUse the `database` kwarg with one of the following patterns:"
                '\ndatabase="database"'
                '\ndatabase=("catalog", "database")'
                '\ndatabase="catalog.database"',
            )
        elif schema is not None:
            table_loc = schema
        elif database is not None:
            table_loc = database
        else:
            table_loc = self.current_database

        table_loc = self._to_sqlglot_table(table_loc)

        conditions = [TRUE]

        if table_loc is not None:
            if (sg_cat := table_loc.args["catalog"]) is not None:
                sg_cat.args["quoted"] = False
            if (sg_db := table_loc.args["db"]) is not None:
                sg_db.args["quoted"] = False
            conditions = [C.table_schema.eq(
                sge.convert(table_loc.sql(self.name)))]

        col = "table_name"
        sql = (
            sg.select(col)
            .from_(sg.table("tables", db="information_schema"))
            .distinct()
            .where(*conditions)
            .sql(self.name)
        )

        with self._safe_raw_sql(sql) as cur:
            out = cur.fetchall()

        return self._filter_with_like(map(itemgetter(0), out), like)

    def execute(
        self, expr: ir.Expr, limit: str | None = "default", **kwargs: Any
    ) -> Any:
        """Execute an expression."""
        self._run_pre_execute_hooks(expr)
        table = expr.as_table()
        sql = self.compile(table, limit=limit, **kwargs)

        schema = table.schema()
        with self._safe_raw_sql(sql) as cur:
            result = self._fetch_from_cursor(cur, schema)
        return expr.__pandas_result__(result)

    def create_table(
        self,
        name: str,
        obj: ir.Table
        | pd.DataFrame
        | pa.Table
        | pl.DataFrame
        | pl.LazyFrame
        | None = None,
        *,
        schema: ibis.Schema | None = None,
        database: str | None = None,
        temp: bool = False,
        overwrite: bool = False,
    ) -> ir.Table:
        if obj is None and schema is None:
            raise ValueError("Either `obj` or `schema` must be specified")

        if database is not None and database != self.current_database:
            raise com.UnsupportedOperationError(
                "Creating tables in other databases is not supported by Postgres"
            )
        else:
            database = None

        properties = []

        if temp:
            properties.append(sge.TemporaryProperty())

        temp_memtable_view = None
        if obj is not None:
            if not isinstance(obj, ir.Expr):
                table = ibis.memtable(obj)
                temp_memtable_view = table.op().name
            else:
                table = obj

            self._run_pre_execute_hooks(table)

            query = self._to_sqlglot(table)
        else:
            query = None

        column_defs = [
            sge.ColumnDef(
                this=sg.to_identifier(colname, quoted=self.compiler.quoted),
                kind=self.compiler.type_mapper.from_ibis(typ),
                constraints=(
                    None
                    if typ.nullable
                    else [sge.ColumnConstraint(kind=sge.NotNullColumnConstraint())]
                ),
            )
            for colname, typ in (schema or table.schema()).items()
        ]

        if overwrite:
            temp_name = util.gen_name(f"{self.name}_table")
        else:
            temp_name = name

        table = sg.table(temp_name, catalog=database,
                         quoted=self.compiler.quoted)
        target = sge.Schema(this=table, expressions=column_defs)

        create_stmt = sge.Create(
            kind="TABLE",
            this=target,
            properties=sge.Properties(expressions=properties),
        )

        this = sg.table(name, catalog=database, quoted=self.compiler.quoted)
        with self._safe_raw_sql(create_stmt) as cur:
            if query is not None:
                insert_stmt = sge.Insert(
                    this=table, expression=query).sql(self.name)
                cur.execute(insert_stmt)

            if overwrite:
                cur.execute(
                    sge.Drop(kind="TABLE", this=this,
                             exists=True).sql(self.name)
                )
                cur.execute(
                    f"ALTER TABLE IF EXISTS {table.sql(self.name)} RENAME TO {this.sql(self.name)}"
                )

        if schema is None:
            # Clean up temporary memtable if we've created one
            # for in-memory reads
            if temp_memtable_view is not None:
                self.drop_table(temp_memtable_view)

            return self.table(name, database=database)

        # preserve the input schema if it was provided
        return ops.DatabaseTable(
            name, schema=schema, source=self, namespace=ops.Namespace(
                database=database)
        ).to_expr()

    def _fetch_from_cursor(self, cursor, schema: sch.Schema) -> pd.DataFrame:
        import pandas as pd

        from ibis.backends.e6data.converter import E6DataPandasData 

        try:
            df = pd.DataFrame.from_records(
                cursor.fetchall(), columns=schema.names, coerce_float=True
            )
        except Exception:
            # clean up the cursor if we fail to create the DataFrame
            #
            # in the sqlite case failing to close the cursor results in
            # artificially locked tables
            cursor.close()
            raise
        df = E6DataPandasData.convert_table(df, schema)
        return df