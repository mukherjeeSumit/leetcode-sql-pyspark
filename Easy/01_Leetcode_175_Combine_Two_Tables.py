from pyspark.sql.functions import * 

# data for person
person = [
( 1, "Wang", "Allen"),
( 2, "Alice", "Bob")
]

# define person schema
person_schema = ["person_id", "last_name", "first_name"]

# create person dataframe
person_df = spark \
.createDataFrame(data=person, schema = person_schema) 

# data for address
address = [
(1, 2, "New York City", "New York"),
(2, 3, "Leetcode", "California" )
]

# define address schema
address_schema = ["address_id", "person_id", "city", "state"]

# create address dataframe
address_df = spark \
.createDataFrame(data=address, schema = address_schema)

# join person and address dataframes
result_df = person_df.alias("person") \
.join(
    address_df.alias("address"), 
    col("person.person_id") == col("address.person_id"),
    "left"
) \
.select(
    col("person.first_name"), 
    col("person.last_name"), 
    col("address.city"), 
    col("address.state")
    ) 

# print result dataframe
result_df.show(truncate=False) 
