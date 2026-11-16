"""AI functions: ember.ai.generate, an LLM call per row.

Like the C++ ember::ai; a str prompt names a column. See
docs/ai-functions.md for endpoints and results.
"""

from ember._ember import ai as _ai

generate = _ai.generate

__all__ = ["generate"]
