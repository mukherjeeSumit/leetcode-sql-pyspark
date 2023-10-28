from pyspark.sql.functions import * 

# data for world
world = [
("Afghanistan", "Asia", 652230, 25500100, 20343000000),
("Albania", "Europe", 28748, 2831741, 12960000000),
("Algeria", "Africa", 2381741, 37100000, 188681000000),
("Andorra", "Europe", 468, 78115, 3712000000),
("Angola", "Africa", 1246700, 20609294, 100990000000)
]

# define world schema
world_schema = ["name", "continent", "area", "population", "gdp"]

# create world dataframe
world_df = spark \
.createDataFrame(data=world, schema = world_schema)

# result dataframe
result_df = world_df.alias("world") \
.select(
    col("world.name"),
    col("world.population"),
    col("world.area")
) \
.filter(
    (col("world.area") >= 3000000) | 
    (col("world.population") >= 25000000)
)

# print result dataframe
result_df.show(truncate=False) 
