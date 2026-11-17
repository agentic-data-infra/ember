"""TopicReport in Python: an LLM labels the documents of a FineWeb-Edu style
corpus (parquet with text, int_score and token_count columns), and the
program reports the topics it found. The Python version of TopicReport.cpp.

    python examples/topic_report.py <path> <url> <model> [docs] [master]

<url> is an OpenAI-compatible API, e.g. http://localhost:8080/v1 for
llama.cpp's llama-server, and <model> the model name it serves. An endpoint
that needs an API key gets it from $LLM_API_KEY, when that is set.

Cheap filters come before the AI columns, so only [docs] documents (100 by
default) reach the model. Each gets a topic and an "educational" flag, as
typed fields; short ones also get a summary, and long ones make no second
call. The labeled rows are cached, so the report and the summaries ask the
model once.
"""

import os
import sys

from ember import Session, ai
from ember.functions import col, concat, countAll, lit, lower, substring, sum, when


def main(argv):
    if len(argv) < 4:
        print(
            "Usage: topic_report.py <file or directory> <url> <model> [docs] [master]",
            file=sys.stderr,
        )
        return 1
    docs = int(argv[4]) if len(argv) > 4 else 100

    builder = (
        Session.builder.appName("TopicReport")
        .master(argv[5] if len(argv) > 5 else "local[*]")
        .config("ember.ai.endpoint.llm.url", argv[2])
        .config("ember.ai.endpoint.llm.model", argv[3])
    )
    if "LLM_API_KEY" in os.environ:
        builder = builder.config("ember.ai.endpoint.llm.api_key_env", "LLM_API_KEY")
    ember = builder.getOrCreate()

    text = substring(col("text"), 1, 2000)
    labeled = (
        ember.read.parquet(argv[1])
        .filter((col("int_score") >= 3) & (col("token_count") < 1000))
        .limit(docs)
        .withColumn(
            "label",
            ai.generate(
                concat(
                    lit(
                        "Pick the topic of this text from: science, health, "
                        "history, technology, society, nature, other. Also say "
                        "whether it teaches something. Text: "
                    ),
                    text,
                ),
                endpoint="llm",
                output={"topic": "varchar", "educational": "boolean"},
            ),
        )
        .withColumn(
            "summary",
            when(
                col("token_count") < 300,
                ai.generate(
                    concat(lit("Summarize in at most 10 words: "), text), endpoint="llm"
                ).getField("result"),
            ).otherwise(lit("")),
        )
        .cache()
    )

    before = ember.stats()
    report = (
        labeled.filter(col("label").getField("status") == "")
        .withColumn("topic", lower(col("label").getField("topic")))
        .groupBy("topic")
        .agg(
            countAll().alias("docs"),
            sum(col("label").getField("educational").cast("bigint")).alias("educational"),
        )
        .orderBy(col("docs").desc(), col("topic").asc())
    )
    print(report.showString(20))
    summaries = labeled.filter(col("summary") != "").select("token_count", "summary")
    print(summaries.showString(5, False))
    failed = labeled.filter(col("label").getField("status") != "").count()
    print(f"Failed rows: {failed}")

    after = ember.stats()
    use = " ".join(f"{k}={v - before.get(k, 0)}" for k, v in sorted(after.items()))
    print(f"Model use: {use}")
    ember.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
