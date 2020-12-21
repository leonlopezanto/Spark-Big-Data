## Dataset "M&Ms" 

The dataset used is data from MnM's stats in USA and it can be downloaded in the repository of this project.

📦 https://github.com/leonlopezanto/Spark-Big-Data/blob/main/MnMs/mnm_dataset.csv 📦

Dependencies of spark and scala added in pom.xml

We explore the use of the cache() function to make memory management more efficient.

1. We use Logger to hide Info and Warn messages and show only Error messages

2. We create session and load the dataset.

3. Queries with added functions.
	-Count (two alternatives with the same result)

	-Filtered using where (Also works with filter)

	-Max and simpler alternative

	-Max, Min, AVG and count set calculation. We show all the dataframe using its size.


The dataset used was obtained from the book "O'Reilly Learning Spark lightning-fast Big Data Analysis". 
In addition, some exercises and their extension into some more complex ones have been inspired by the examples in this book.