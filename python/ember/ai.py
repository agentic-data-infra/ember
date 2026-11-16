"""AI functions: ember.ai.generate, an LLM call per row.

Like the C++ ember::ai; a str prompt names a column. Without output,
generate returns a struct of result and status; with output, a dict of field
names to type names (or to dicts, for nested structs), the model answers in
that schema and the struct has those fields plus status:

    g = ai.generate("text", endpoint="small",
                    output={"educational": "boolean", "topic": "varchar"})
    df.filter(g.getField("educational"))

See docs/ai-functions.md for endpoints and results.
"""

from ember._ember import ai as _ai

generate = _ai.generate

__all__ = ["generate"]
