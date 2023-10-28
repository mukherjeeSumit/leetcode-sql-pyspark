from pyspark.sql.functions import * 

# data for employee
employee = [
( 3, "Brad", None , 4000),
( 1, "John", 3, 1000),
(2, "Dan", 3, 2000),
(4, "Thomas", 3, 4000)
]

# define employee schema
employee_schema = ["employee_id", "name", "supervisor", "salary"]

# create employee dataframe
employee_df = spark \
.createDataFrame(data=employee, schema = employee_schema)

# data for bonus
bonus = [
(2, 500),
(4, 2000)
]

# define bonus schema
bonus_schema = ["employee_id", "bonus"]

# create bonus dataframe
bonus_df = spark \
.createDataFrame(data=bonus, schema = bonus_schema) 

# join employee and bonus dataframes
result_df = employee_df.alias("employee") \
.join(bonus_df.alias("bonus"), col("employee.employee_id") == col("bonus.employee_id"),"left") \
.select(
    col("employee.name"), 
    col("bonus.bonus")
    ) \
.filter((col("bonus.bonus") < 1000) | (col("bonus.bonus").isNull()))

# print result dataframe
result_df.show(truncate=False) 
