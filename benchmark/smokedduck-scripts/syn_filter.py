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

k = 1000000


create_table_sql = f"""
CREATE TABLE scatter_table (
    id INTEGER PRIMARY KEY,
    fcol INTEGER
);
"""
con.execute(create_table_sql)


# 动态生成 INSERT 语句
insert_sql = "INSERT INTO scatter_table (id, fcol) VALUES "
values = ", ".join([f"({i}, FLOOR(1 + RANDOM() * 1000))" for i in range(0, k)])
insert_sql += values + ";"

con.execute(insert_sql)

order_create_sql = """
CREATE TABLE sort_table AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY fcol) - 1 AS id, 
    fcol
FROM 
    scatter_table
ORDER BY 
    fcol;
"""

con.execute(order_create_sql)

tables = con.execute("PRAGMA show_tables").fetchdf()
print(tables)

# select_sql = "SELECT * FROM scatter_table;"
# df = con.execute(select_sql).fetchdf()
# print(df)

# select_order_sql = "SELECT * FROM sort_table;"
# df = con.execute(select_order_sql).fetchdf()
# print(df)
# con.execute("PRAGMA disable_optimizer")
con.execute("PRAGMA enable_lineage")
con.execute("PRAGMA persist_lineage")
con.execute("PRAGMA compress_lineage")

# df = con.execute(select_sql).fetchdf()
conditions = " OR ".join([f"(fcol >= {4*n+1} AND fcol <= {4*n+2})" for n in range(25)])
# select_sql = f"SELECT * FROM sort_table WHERE {conditions};"
select_sql = f"SELECT * FROM scatter_table WHERE {conditions};"
# print(select_sql)

df = con.execute(select_sql).fetchdf()

tables = con.execute("PRAGMA show_tables").fetchdf()
print(tables)

lineage_size, _, _, _, _, _ = getStats(con, select_sql)

# sql = "SELECT * FROM LINEAGE_3_FILTER_1;"
# df = con.execute(sql).fetchdf()
# print(df)

print("Lineage size: ", lineage_size)

con.execute("PRAGMA disable_lineage")
con.execute("DROP TABLE scatter_table")
con.execute("DROP TABLE sort_table")


