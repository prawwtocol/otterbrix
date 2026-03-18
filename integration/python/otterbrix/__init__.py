_exported_symbols = []


# Classes (loaded first to avoid circular imports with experimental)
from .otterbrix import (
    OtterBrixPyConnection,
    OtterBrixPyRelation,
    Expression,
    ConstantExpression,
    ColumnExpression,
    CountExpression,
)
_exported_symbols.extend([
    "OtterBrixPyConnection",
    "OtterBrixPyRelation",
    "Expression",
    "ConstantExpression",
    "ColumnExpression",
    "CountExpression",
])

# Type constructors
from .otterbrix import (
    sqltype,
    dtype,
    type,
    array_type,
    list_type,
    union_type,
    string_type,
    enum_type,
    decimal_type,
    struct_type,
    row_type,
    map_type,
)
_exported_symbols.extend([
    "sqltype",
    "dtype",
    "type",
    "array_type",
    "list_type",
    "union_type",
    "string_type",
    "enum_type",
    "decimal_type",
    "struct_type",
    "row_type",
    "map_type",
])

# Connection factory
from .otterbrix import connect
_exported_symbols.extend([
    "connect",
])

# Backwards compatibility: old SQL-based bindings
try:
    from .otterbrix import Client, Connection, Cursor, to_aggregate
    _exported_symbols.extend([
        "Client",
        "Connection",
        "Cursor",
        "to_aggregate",
    ])
except ImportError:
    pass

# Modules (after C++ extension symbols so submodules can import them)
import otterbrix.typing as typing
import otterbrix.experimental as experimental

_exported_symbols.extend([
    "typing",
    "experimental",
])

__all__ = _exported_symbols
