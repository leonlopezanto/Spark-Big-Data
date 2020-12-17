## Dataset "Padron" (Madrid Census)

The dataset used is data from the census of Madrid in 2020 and can be downloaded at the following link.

📦 https://datos.madrid.es/egob/catalogo/200076-1-padron 📦 

This dataset is avalaible too in project path "/Padron/padron202011.csv"
Dependencies of spark and scala added in pom.xml

1. We have loaded the Padron dataset into a DataFrame.
2. We have made trim on the columns of the dataset.
3. We have eliminated the null records.
4. We have made some queries:
5. National men and women and foreign men and women grouped by district and neighborhood.
6. Total people in each district.
7. We have saved the resulting dataset in formpato Parquet.


In this project we have edited the pom.xml to add some Spark dependencies.
You can find the source of our project in the path: Padron/src/main/scala/com/LeonLopez/app.scala

