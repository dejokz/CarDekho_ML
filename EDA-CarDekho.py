# Databricks notebook source
# MAGIC %md
# MAGIC ## Reading the CSV into spark dataframe

# COMMAND ----------

file_path = '/FileStore/CarDekho/Car_details_v3.csv'

# COMMAND ----------

df = spark.read.csv(file_path3, header = True)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Missing columns: Mileage: 2.72%  
# MAGIC Engine: 2.72%  
# MAGIC Torque: 2.73%  
# MAGIC Seats: 2.72%  

# COMMAND ----------

# MAGIC %md
# MAGIC Things to do:  
# MAGIC 1. ~Remove CC from `Engine` and cast it to int/double~
# MAGIC 2. ~Cast `selling_price` to int/double~
# MAGIC 3. ~Cast `km_driven` to int~
# MAGIC 4. ~Rename `year` to `age` cast it to int~
# MAGIC 5. ~Remove 'kmpl' from mileage and cast it to double~  
# MAGIC 6. ~Remove bhp from `max_power` and cast it to int~
# MAGIC 7. ~Cast `seats` to int~

# COMMAND ----------

from pyspark.sql.functions import col, translate

# COMMAND ----------

df = (df.withColumn("selling_price", col("selling_price").cast("int"))
        .withColumn("km_driven", col("km_driven").cast("int"))
        .withColumn("seats", col("seats").cast("int"))
     )

# COMMAND ----------

display(df)

# COMMAND ----------

df = (df.withColumn("engine", translate(col("engine"), "CC", "").cast("double"))
        .withColumn("mileage", translate(col("mileage"), "kmpl", "").cast("double"))
        .withColumn("max_power", translate(col("max_power"), "bhp", "").cast("double"))
     )

# COMMAND ----------

display(df)

# COMMAND ----------

df.selectExpr("max(year)").display()

# COMMAND ----------

df = df.withColumn("year", "2020" - col("year"))

# COMMAND ----------

df = df.withColumnRenamed("year", "age")

# COMMAND ----------


1,45,500
1,00,00,000

# COMMAND ----------

display(df)

# COMMAND ----------

df.groupBy("age").count().orderBy("age").display()

# COMMAND ----------

df.groupBy("seller_type").count().display()

# COMMAND ----------

df.selectExpr("*").filter(col("selling_price") = max(col("selling_price")))

# COMMAND ----------

df.agg({'selling_price': 'max'}).show()


# COMMAND ----------

df.cache()

# COMMAND ----------

df.filter(col("selling_price") == 10000000).display()

# COMMAND ----------

df.agg({'mileage': 'min'}).show()

# COMMAND ----------

df.filter(col("mileage") == 0.0).display()

# COMMAND ----------

df = df.drop("torque")

# COMMAND ----------


