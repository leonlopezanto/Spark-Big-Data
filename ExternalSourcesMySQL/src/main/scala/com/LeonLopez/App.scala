package com.LeonLopez

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SparkSession}

/**
 * @author: Leon Lopez
 * @description: Obtaining data from external sources using the MySQL driver and window functions.
 *         Dataset: https://github.com/datacharmer/test_db
 *
 */
object App {

  def main(args: Array[String]) {

    //Stop warning and error messages
    //Apache
    Logger.getLogger("org.apache").setLevel(Level.ERROR)
    //Spark Project
    Logger.getLogger("org.spark-project").setLevel(Level.ERROR)

    val spark: SparkSession = SparkSession.builder() //SparkSession
      .master("local[1]") //Number of cores
      .appName("External Sources") //App Name
      .getOrCreate()

    //JDBC Details
    //val driver = "com.mysql.jdbc.Driver" //Not necessary
    val url = "jdbc:mysql://localhost:3306/employees"
    val user = "root"
    val pass = "1234"

    //Connect to JDBC and load table in DataFrame
    //Read employees data
    val employees = spark.read.format("jdbc")
      //.option("driver", driver)
      .option("url", url)
      .option("dbtable", "employees")
      .option("user", user)
      .option("password", pass)
      .load()

    employees.show()

    //Read departments data
    val departments = spark.read.format("jdbc")
      .option("url", url)
      .option("dbtable", "departments")
      .option("user", user)
      .option("password", pass)
      .load()

    departments.show()

    //Read department manager data
    val dept_m = spark.read.format("jdbc")
      .option("url", url)
      .option("dbtable", "dept_manager")
      .option("user", user)
      .option("password", pass)
      .load()

    dept_m.show()

    //Read salaries data
    val salaries = spark.read.format("jdbc")
      .option("url", url)
      .option("dbtable", "salaries")
      .option("user", user)
      .option("password", pass)
      .load()

    salaries.show()

    //Join department and employees
    val depNameEmp = departments.as("Dep")
      .join(dept_m.as("DM"), col("Dep.dept_no") === col("DM.dept_no"))
      .select("emp_no", "Dep.dept_name", "from_date", "to_date")

    //Join employees with depNameEmp table
    val empDep = employees.as("Emp")
      .join(depNameEmp.as("Dne"), col("Dne.emp_no") === col("Emp.emp_no"))
      .select("Dne.dept_name", "Emp.first_name", "Emp.last_name", "Emp.gender", "Dne.emp_no")

    empDep.show()

    //Join salaries with employees department table
    val empDepSal = salaries.as("S")
      .join(empDep.as("ED"), col("ED.emp_no") === col("S.emp_no"))
      .select("ED.emp_no", "S.salary", "ED.dept_name", "ED.first_name", "ED.last_name")

    empDepSal.show()

    //With dense_rank and rank we get the names of the people who earn the most from each department.
    //Create a temporal view
    empDepSal.createOrReplaceTempView("empDepSal")

    spark.sql(
      """
        |SELECT dept_name, first_name, last_name, salary, rank
        |FROM (
        |   SELECT dept_name, first_name, last_name, salary, dense_rank()
        |   OVER (PARTITION BY dept_name ORDER BY salary DESC) as rank
        |   FROM empDepSal
        |
        |) t
        |WHERE rank <= 1
        |Order by dept_name DESC
        |""".stripMargin).show(false)

    //Select the last 3 salary updates for each employee ordered by emp_no
    val salEmp = employees.as("Emp")
      .join(salaries.as("Sal"), col("Sal.emp_no") === col("Emp.emp_no"))
      .select("Sal.emp_no", "Emp.first_name", "Emp.last_name", "Sal.salary", "Sal.from_date", "Sal.to_date")

    salEmp.createOrReplaceTempView("SalaryEmployee")

    spark.sql(
      """
        |SELECT emp_no, CONCAT(first_name, ' ', last_name), from_date, salary, rank
        |FROM(
        |   SELECT emp_no, first_name, last_name, from_date, salary, dense_rank()
        |   OVER (PARTITION BY emp_no ORDER BY from_date DESC) as rank
        |   FROM SalaryEmployee
        |)
        |WHERE rank <= 3
        |ORDER BY emp_no ASC, from_date ASC
        |""".stripMargin).show()

  }
}