from pyspark.sql.functions import * 

# data for employee
employee = [
(1, "Joe", 70000, 1),
(2, "Jim", 90000, 1),
(3, "Henry", 80000, 2),
(4, "Sam", 60000, 2),
(5, "Max", 90000, 1)
]

# define employee schema
employee_schema = ["id", "name", "salary", "department_id"]

# create employee dataframe
employee_df = spark \
.createDataFrame(data=employee, schema = employee_schema)

# data for department
department = [
(1, "IT"),
(2, "Sales")
]

# define department schema
department_schema = ["id", "name"]

# create department dataframe
department_df = spark \
.createDataFrame(data=department, schema = department_schema) 

# create temp view for Employee Dataframe
employee_sql_df = employee_df \
                    .createOrReplaceTempView("EMPLOYEE")

# create temp view for department dataframe
department_sql_df = department_df \
                    .createOrReplaceTempView("DEPARTMENT")

# join employee and department temp views
result_df = spark.sql("""SELECT d.name AS Department, e.name AS Employee, e.salary AS Salary FROM EMPLOYEE e JOIN DEPARTMENT d ON e.department_id = d.id  WHERE (department_id, salary) IN (SELECT department_id, MAX(salary) FROM EMPLOYEE GROUP BY department_id)""")

# print result dataframe
result_df.show(truncate=False)
