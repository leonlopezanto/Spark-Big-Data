# Padron
  
The dataset used is data from the census of Madrid in 2020 and can be downloaded at the following link.

📦 https://datos.madrid.es/egob/catalogo/200076-1-padron 📦 

This dataset is avalaible too in project path "/Padron/padron202011.csv"
Dependencies of spark and scala added in pom.xml

We have loaded the Padron dataset into a DataFrame.
We have made trim on the columns of the dataset.
We have eliminated the null records.
We have made some queries:
National men and women and foreign men and women grouped by district and neighborhood.
Total people in each district.
We have saved the resulting dataset in formpato Parquet.


In this project we have edited the pom.xml to add some Spark dependencies.
You can find the source of our project in the path: Padron/src/main/scala/com/LeonLopez/app.scala

