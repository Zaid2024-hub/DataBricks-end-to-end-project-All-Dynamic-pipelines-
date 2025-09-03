# Databricks notebook source
# Catalog Name
catalog = "workspace"

# Source Schema
source_schema = "silver"

# Source Object
source_object = "silver_bookings"

#CDC Column
cdc_col = "modifiedDate"

#Backdated refresh
backdated_refresh = ""

#Source Fact Table
fact_table = f"{catalog}.{source_schema}.{source_object}"
 
# Target Schema
target_schema = "gold"

# Target Object
target_object = "FactBookings"

# Fact Key Cols List
fact_key_cols = ["DimPassengersKey","DimFlightsKey","DimAirportsKey","booking_date"]



# COMMAND ----------

fact_key_cols_str = " AND ".join([f"src.{col} = trg.{col}" for col in fact_key_cols])


# COMMAND ----------

dimensions = [
    {
        "table": f"{catalog}.{target_schema}.DimPassengers",
        "alias": "DimPassengers",
        "join_keys": [("passenger_id", "passenger_id")] #(fact_col,dim_col)
    },
    {
        "table": f"{catalog}.{target_schema}.DimFlights", # (fact_col, dim_col)
        "alias": "DimFlights",
        "join_keys": [("flight_id", "flight_id")]
    },
    {
        "table": f"{catalog}.{target_schema}.DimAirports", # (fact_col, dim_col)
        "alias": "DimAirports",
        "join_keys": [("airport_id", "airport_id")]
    },
]

#Columns you want to keep from Fact table (besides the surrogate keys)
fact_columns = ["amount","booking_date","modifiedDate"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## ** LAST LOAD DATE **

# COMMAND ----------

# No back Dated Refresh
if len(backdated_refresh) == 0:

  #If Table Exists In the Destination
  if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):

    last_load = spark.sql(f"SELECT max({cdc_col}) FROM workspace.{target_schema}.{target_object}").collect()[0][0]
  else:
    last_load = "1900-01-01 00:00:00"

else:
  last_load = backdated_refresh

last_load

# COMMAND ----------

# MAGIC %md
# MAGIC #### ** DYNAMIC FACT QUERY [BRING KEYS] **

# COMMAND ----------

def generate_fact_query_incremental(fact_table, dimensions, fact_columns, cdc_col, last_load):
    fact_alias = "f"

    # Base columns to select
    select_cols = [f"{fact_alias}.{col}" for col in fact_columns]

    # Build joins dynamically
    join_clauses = []
    for dim in dimensions:
        table_full = dim["table"]
        alias = dim["alias"]
        table_name = table_full.split(".")[-1]  # fixed split here too
        surrogate_key = f"{alias}.{table_name}Key"
        select_cols.append(surrogate_key)

        # Build ON clause
        on_condition = [
            f"{fact_alias}.{fk} = {alias}.{dk}" for fk, dk in dim["join_keys"]
        ]
        join_clause = f"LEFT JOIN {table_full} {alias} ON " + " AND ".join(on_condition)
        join_clauses.append(join_clause)

    # Final SELECT and JOIN clauses
    select_clause = ",\n  ".join(select_cols)
    joins = "\n".join(join_clauses)

    # WHERE clause for incremental filtering
    where_clause = f"{fact_alias}.{cdc_col} >= DATE('{last_load}')"

    # Final query
    query = f"""
SELECT
  {select_clause}
FROM {fact_table} {fact_alias}
{joins}
WHERE {where_clause}
""".strip()

    return query




# COMMAND ----------

query = generate_fact_query_incremental(fact_table, dimensions, fact_columns, cdc_col, last_load)
print(query)


# COMMAND ----------

df_fact = spark.sql(query)

# COMMAND ----------

# MAGIC %md
# MAGIC #### ** UPSERT **

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df_fact.filter((col('DimPassengersKey')==190)& (col('DimFlightsKey')==68)& (col('DimAirportsKey')==27)).display()

# COMMAND ----------

#Fact Key Columns Merge Condition
fact_key_cols_str

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):

    dlt_obj = DeltaTable.forName(spark, f"{catalog}.{target_schema}.{target_object}")
    dlt_obj.alias("trg").merge(df_fact.alias("src"), fact_key_cols_str)\
                        .whenMatchedUpdateAll(condition = f"src.{cdc_col} >= trg.{cdc_col}")\
                        .whenNotMatchedInsertAll()\
                        .execute()
else:

    df_fact.write.format("delta")\
            .mode("append")\
            .saveAsTable(f"{catalog}.{target_schema}.{target_object}")

# COMMAND ----------

