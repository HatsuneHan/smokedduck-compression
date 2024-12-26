import json
import duckdb
import pandas as pd
import argparse
import csv
import os
import re

from utils import parse_plan_timings, parse_plan_cardinality, Run, DropLineageTables, getStats

con = duckdb.connect(database=':memory:', read_only=False)
# con.execute("CALL dbgen(sf="+str(sf)+");")

create_table_sql = f"""
CREATE TABLE group_table (
    id INTEGER PRIMARY KEY,
    fcol INTEGER
);
"""
con.execute(create_table_sql)


# 动态生成 INSERT 语句
k = 10000
insert_sql = "INSERT INTO group_table (id, fcol) VALUES "
values = ", ".join(
    [f"({i}, FLOOR(1 + RANDOM() * 100))" for i in range(0, k)])
insert_sql += values + ";"

con.execute(insert_sql)

tables = con.execute("PRAGMA show_tables").fetchdf()
print(tables)

# select_sql = "SELECT * FROM group_table;"
# df = con.execute(select_sql).fetchdf()
# print(df)

con.execute("PRAGMA disable_optimizer")
con.execute("PRAGMA enable_lineage")
con.execute("PRAGMA persist_lineage")
con.execute("PRAGMA compress_lineage")

# # df = con.execute(select_sql).fetchdf()
# conditions = " OR ".join([f"(fcol >= {4*n+1} AND fcol <= {4*n+2})" for n in range(25)])
# # select_sql = f"SELECT * FROM sort_table WHERE {conditions};"
# select_sql = f"SELECT * FROM scatter_table WHERE {conditions};"
# # print(select_sql)

group_filter_sql = f"SELECT avg(id) FROM group_table WHERE fcol >= 50 GROUP BY fcol;"
df = con.execute(group_filter_sql).fetchdf()
# print(df)

filter_sql = f"SELECT * FROM group_table WHERE fcol >= 50;"
df = con.execute(filter_sql).fetchdf()
# # print(df)

con.execute("PRAGMA disable_lineage")

# print(con.execute("PRAGMA show_tables").fetchdf())

# tables = con.execute("PRAGMA show_tables").fetchdf()
# print(tables)

# con.execute("PRAGMA clear_lineage")

lineage_size, _, _, _, _, _ = getStats(con, group_filter_sql)


lineage_size, _, _, _, _, _ = getStats(con, filter_sql)

# # sql = "SELECT * FROM LINEAGE_3_FILTER_1;"
# # df = con.execute(sql).fetchdf()
# # print(df)

# print("Lineage size: ", lineage_size)


