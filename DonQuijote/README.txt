## Dataset "Don Quixote" (Famous spanish book Don Quixote)

The dataset used is a txt file containing the famous literary work "Don Quixote". 
It could be downloaded there:
📦 https://gist.github.com/jsdario/6d6c69398cb0c73111e49f1218960f79 📦

This dataset is avalaible too in project root.
 Dependencies of spark and scala added in pom.xml

1. We have loaded the DonQuixote dataset into a DataFrame.
2. We have made trim on the columns of the dataset.
3. We have eliminated the null records.
4. Some queries:
    -National men and women and foreign men and women grouped by district and neighborhood.
    -Total people in each district.
    -We have saved the resulting dataset in formpato Parquet.
5.We have saved the resulting dataset in Parquet format.

In this project we have edited the pom.xml to add some Spark dependencies. You can find the source of our project in the path: Padron/src/main/scala/com/LeonLopez/app.scala