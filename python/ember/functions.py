"""Column functions: ember.functions.col, lit, explode, sum, ...

Like the C++ ember::functions; a str argument names a column.
"""

from ember._ember import functions as _functions

globals().update(
    {name: getattr(_functions, name) for name in dir(_functions) if not name.startswith("_")}
)

__all__ = [name for name in dir(_functions) if not name.startswith("_")]
