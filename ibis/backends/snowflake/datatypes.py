from __future__ import annotations

import sqlalchemy.types as sat
from snowflake.sqlalchemy import (
    ARRAY,
    OBJECT,
    TIMESTAMP_LTZ,
    TIMESTAMP_NTZ,
    TIMESTAMP_TZ,
    VARIANT,
)
from sqlalchemy.ext.compiler import compiles

import ibis.expr.datatypes as dt
from ibis.backends.base.sql.alchemy.datatypes import AlchemyType


@compiles(sat.NullType, "snowflake")
def compiles_nulltype(element, compiler, **kw):
    return "VARIANT"


_SNOWFLAKE_TYPES = {
    "REAL": dt.float64,
    "TEXT": dt.string,
    "DATE": dt.date,
    "TIMESTAMP": dt.timestamp,
    "VARIANT": dt.json,
    "TIMESTAMP_LTZ": dt.timestamp,
    "TIMESTAMP_TZ": dt.Timestamp("UTC"),
    "TIMESTAMP_NTZ": dt.timestamp,
    "OBJECT": dt.Map(dt.string, dt.json),
    "ARRAY": dt.Array(dt.json),
    "BINARY": dt.binary,
    "TIME": dt.time,
    "BOOLEAN": dt.boolean,
}


def parse(
    text: str, *, precision: int | None = None, scale: int | None = None
) -> dt.DataType:
    """Parse a Snowflake type into an ibis data type."""
    if text == "FIXED":
        if (precision is None and scale is None) or (precision and not scale):
            return dt.int64
        else:
            return dt.Decimal(precision=precision or 38, scale=scale or 0)
    elif text.startswith("TIMESTAMP"):
        # timestamp columns have a specified scale, defaulting to 9
        return _SNOWFLAKE_TYPES[text].copy(scale=scale or 9)
    return _SNOWFLAKE_TYPES[text]


class SnowflakeType(AlchemyType):
    dialect = "snowflake"

    @classmethod
    def from_ibis(cls, dtype):
        if dtype.is_array():
            return ARRAY
        elif dtype.is_map() or dtype.is_struct():
            return OBJECT
        elif dtype.is_json():
            return VARIANT
        elif dtype.is_timestamp():
            if dtype.timezone is None:
                return TIMESTAMP_NTZ
            else:
                return TIMESTAMP_TZ
        elif dtype.is_string():
            # 16MB
            return sat.VARCHAR(2**24)
        elif dtype.is_binary():
            # 8MB
            return sat.VARBINARY(2**23)
        else:
            return super().from_ibis(dtype)

    @classmethod
    def to_ibis(cls, typ, nullable=True):
        if isinstance(typ, (sat.REAL, sat.FLOAT, sat.Float)):
            return dt.Float64(nullable=nullable)
        elif isinstance(typ, TIMESTAMP_NTZ):
            return dt.Timestamp(timezone=None, nullable=nullable)
        elif isinstance(typ, (TIMESTAMP_LTZ, TIMESTAMP_TZ)):
            return dt.Timestamp(timezone="UTC", nullable=nullable)
        elif isinstance(typ, ARRAY):
            return dt.Array(dt.json, nullable=nullable)
        elif isinstance(typ, OBJECT):
            return dt.Map(dt.string, dt.json, nullable=nullable)
        elif isinstance(typ, VARIANT):
            return dt.JSON(nullable=nullable)
        elif isinstance(typ, sat.Numeric):
            if (scale := typ.scale) == 0:
                # kind of a lie, should be int128 because 38 digits
                return dt.Int64(nullable=nullable)
            else:
                return dt.Decimal(
                    precision=typ.precision or 38,
                    scale=scale or 0,
                    nullable=nullable,
                )
        else:
            return super().to_ibis(typ, nullable=nullable)
