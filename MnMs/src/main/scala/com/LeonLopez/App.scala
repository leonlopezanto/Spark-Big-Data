package com.LeonLopez

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * @author ${user.name}
 * @description: First steps with  aggregation functions.
 * @dataset: You can get the MnM's dataset in the root of this repository
 */
object App {

  def main(args : Array[String]) {

    //No INFO and WARN messages
    Logger.getLogger("org.apache").setLevel(Level.ERROR)
    Logger.getLogger("org.spark-project").setLevel(Level.ERROR)

    //Create Spark Session
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("MnMs")
      .getOrCreate()

    //Load Dataset
    val path = "mnm_dataset.csv"
    val mnmDF = spark.read.format("csv")
      .option("header", true)
      .option("inferSchema", "true")
      .load(path).cache()

    mnmDF.printSchema()

    //Some queries with aggregate functions
    //Count of all colors group by State and Color in DESC order
    //1º Count without agg function
    val mnmCount1 = mnmDF.select("State", "Color", "Count")
      .groupBy("Color", "State")
      .count()
      .orderBy(desc("Count")).cache()

    mnmCount1.show()

    //2º Count with agg function
    val mnmCount2 = mnmDF.select("State", "Color", "Count")
      .groupBy("Color", "State")
      .agg(count("Color").alias("Total"))
      .orderBy(desc("Total")).show()

    //Filtering colors just in California "CA"
    val mnmCalifornia = mnmCount1.where(col("State") === "CA").cache()
    //Also: val mnmCalifornia = mnmCount1.filter(col("State") === "CA")
    mnmCalifornia.show(false)

    //Color of Max count of MnMs in California
    mnmCalifornia.select(col("Color"), col("count"))
      .groupBy("Color")
      .agg(max("count")).show()

    //Faster and simple but the best alternative
    mnmCalifornia.select(col("Color"), col("count").alias("Max in California")).limit(1).show()

    //Calculate Max, Min, AVG, Count in the same operation.
    val resume = mnmDF.groupBy("Color", "State")
      .agg(avg("Count").alias("Average"), max("Count"), min("count"))
      .show(mnmDF.count().asInstanceOf[Int]) //Spark has note the "*" possibility of SQL so if you want to show the whole dataset you have to use the size of dataframe
    

  }

}