import json
import duckdb
import pandas as pd
import argparse
import csv
import os
import re

from utils import parse_plan_timings, parse_plan_cardinality, Run, DropLineageTables, getStats
from get_workload import get_workload

con = duckdb.connect(database=':flights:', read_only=False)

drop_table_sql = "DROP TABLE IF EXISTS tbl_flights;"
con.execute(drop_table_sql)

create_table_sql = f"""
CREATE TABLE tbl_flights AS
    SELECT * FROM 'dataset.csv';
"""
con.execute(create_table_sql)

workload_list = get_workload("/home/hxy/Documents/IDEBench-public/results")
print(workload_list)

# con.execute("PRAGMA disable_optimizer")
con.execute("PRAGMA enable_lineage")
con.execute("PRAGMA persist_lineage")
con.execute("PRAGMA compress_lineage")

total_lineage_list = []
lineage_list = []

# for i in range(len(workload_list)):
for i in range(1):

  lineage_list = []
  for j in range(len(workload_list[i])):
    sql = workload_list[i][j]
    df = con.execute(sql).fetchdf()
    lineage_size, _, _, _, _, _ = getStats(con, sql)
    lineage_list.append(lineage_size)

  total_lineage_list.append(lineage_list)

print(total_lineage_list)
    








