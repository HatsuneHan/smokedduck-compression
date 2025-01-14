import os
import json
from datetime import datetime

def read_json_files_in_order(directory):
  files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith('.json')]
  files.sort(key=os.path.getctime)

  file_list = []
  
  for file in files:
    with open(file, 'r', encoding='utf-8') as f:
      try:
        data = json.load(f)
        print(f"Reading {file} created on {datetime.fromtimestamp(os.path.getctime(file))}:")
        file_list.append(data)
      except json.JSONDecodeError as e:
        print(f"Error decoding JSON in file {file}: {e}")

  return file_list

def get_workload(directory):
  json_list = read_json_files_in_order(directory)
  workload_list = []

  for data in json_list:
    sql_list = []
    for i in range(0, len(data['results'])):
      sql_list.append(data['results'][str(i)]['sql'])
    workload_list.append(sql_list)

  return workload_list