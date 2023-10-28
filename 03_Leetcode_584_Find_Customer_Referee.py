from pyspark.sql.functions import * 

# data for customer
customer = [
(1, "Will", None),
(2, "Jane", None),
(3, "Alex", 2),
(4, "Bill", None),
(5, "Zack", 1),
(6, "Mark", 2)
]

# define customer schema
customer_schema = ["id", "name", "referee_id"]

# create customer dataframe
customer_df = spark \
.createDataFrame(data=customer, schema = customer_schema)

# result dataframe
result_df = customer_df.alias("customer") \
.select(col("customer.name")) \
.filter((col("customer.referee_id") != 2) | (col("customer.referee_id").isNull()))

# print result dataframe
result_df.show(truncate=False) 
