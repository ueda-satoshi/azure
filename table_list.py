# Databricks notebook source
import pprint

db_list_db = spark.sql("show databases")
db_list_dn = db_list_db.select('databaseName').toPandas()
db_list = db_list_dn.values.tolist()

for i in db_list:
  query = "show tables from " + i[0]
  tables_q = spark.sql(query)
  tables_t = tables_q.select('database','tableName').toPandas()
  tables_n = tables_t.values.tolist()
  pprint.pprint(tables_n)

# COMMAND ----------


