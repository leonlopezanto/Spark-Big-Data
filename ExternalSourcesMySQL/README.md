## Using data from External Sources (MySQL)

The dataset used is the Employees sample database. The Employees sample database was developed by Patrick Crews and Giuseppe Maxia and provides a combination of a large base of data (approximately 160MB) spread over six separate tables and consisting of 4 million records in total.
The structure is compatible with a wide range of storage engine types. Through an included data file, support for partitioned tables is also provided. 

📦 https://github.com/datacharmer/test_db 📦 

1. Creating SparkContext 
2. Details for connection with MySQL 
3. Getting tables:

-Loading Employees table

-Loading Deparments table

-Loading Department Manager table

-Loading Salaries table
4. Query to know the people who earn more in each department
-Join tables Department and Employees
-Join table Employes and join above
-Join table Salaries with previous Joins
-Query using window functions (rank and dense_rank)
5. Query to know the last 3 salary updates for each employee
-Join table Salaries and Employees
-Consult with window functions (rank and dense_rank)


Dependencies of spark, scala and drivers installations to connect the app with MySQL have been added in pom.xml
You can find the relational diagram of the database in the project root.
You can find the source of our project in the path: ExternalSourcesMySQL/src/main/scala/com/LeonLopez/app.scala
