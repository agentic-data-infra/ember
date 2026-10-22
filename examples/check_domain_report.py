"""Checks domain_report's output against the same report computed by DuckDB.

    domain_report <parquet> local[*] 1 100000000 > report.txt
    python3 check_domain_report.py <parquet> report.txt

Needs the duckdb Python package. Compares every domain row (in order) and the
totals; averages and rates may differ in the last printed digit.
"""

import sys

import duckdb

MIN_SCORE = 4
HOST = r"^[A-Za-z][A-Za-z0-9+.-]*://(?:www\.)?([^/:?#]+)"


def parse_tables(path):
    """Returns the tables printed by Dataframe.show() as lists of row lists."""
    tables, rows, border = [], None, 0
    for line in open(path):
        line = line.rstrip("\n")
        if line.startswith("+-"):
            border += 1
            if border == 1:
                rows = []
            elif border == 3:
                tables.append(rows[1:])  # drop the header
                border = 0
        elif line.startswith("|") and rows is not None and border:
            rows.append([cell.strip() for cell in line[1:-1].split("|")])
    return tables


def number(cell):
    return None if cell == "NULL" else float(cell)


def main(parquet, report):
    con = duckdb.connect()
    con.execute(
        f"""
        CREATE VIEW docs AS
        SELECT lower(regexp_extract(url, '{HOST}', 1)) AS domain, score,
               int_score, token_count
        FROM read_parquet('{parquet}')"""
    )
    expected = con.execute(
        f"""
        WITH a AS (SELECT domain, count(*) AS docs, sum(token_count) AS tokens
                   FROM docs GROUP BY domain),
             e AS (SELECT domain, count(*) AS edu_docs,
                          sum(token_count) AS edu_tokens, avg(score) AS s
                   FROM docs WHERE int_score >= {MIN_SCORE} GROUP BY domain)
        SELECT a.domain, docs, coalesce(edu_docs, 0) AS edu_docs,
               coalesce(edu_docs, 0) / docs AS keep_rate,
               coalesce(edu_tokens, 0), tokens, s
        FROM a LEFT JOIN e USING (domain)
        ORDER BY edu_docs DESC, docs DESC, a.domain"""
    ).fetchall()
    totals = con.execute(
        f"""
        SELECT count(*), sum(token_count), count(DISTINCT domain),
               count(*) FILTER (WHERE int_score >= {MIN_SCORE}),
               sum(token_count) FILTER (WHERE int_score >= {MIN_SCORE})
        FROM docs"""
    ).fetchone()

    report_rows, total_rows = parse_tables(report)
    errors = []
    if len(report_rows) != len(expected):
        errors.append(f"{len(report_rows)} domains, expected {len(expected)}")
    for i, (got, want) in enumerate(zip(report_rows, expected)):
        domain, docs, edu_docs, rate, edu_tokens, tokens, avg = want
        ok = (
            got[0] == domain
            and int(got[1]) == docs
            and int(got[2]) == edu_docs
            and abs(number(got[3]) - rate) <= 0.0006
            and int(got[4]) == edu_tokens
            and int(got[5]) == tokens
            and (
                (avg is None and got[6] == "NULL")
                or (avg is not None and abs(number(got[6]) - avg) <= 0.0051)
            )
        )
        if not ok:
            errors.append(f"row {i}: got {got}, expected {list(want)}")
    if [int(c) for c in total_rows[0]] != [int(v) for v in totals]:
        errors.append(f"totals: got {total_rows[0]}, expected {list(totals)}")

    for error in errors[:20]:
        print(error)
    print(f"{len(expected)} domains, totals {list(totals)}: "
          + ("MISMATCH" if errors else "OK"))
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1], sys.argv[2]))
