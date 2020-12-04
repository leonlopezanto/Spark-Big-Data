package com.josedeveloper

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.ColumnName

/**
 * @author ${user.name}
 */
object App {

  def main(args : Array[String]) {

    //Create Spark Session
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]") //Number of cores
      .appName("Padron.com") //App Name
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")//Stop Info messages please!

    println("Creating DF")
    //Read the CSV. https://datos.madrid.es/egob/catalogo/200076-1-padron.csv
    val padronPath = "./padron202011.csv"

    //Create spark dataframe reading from local
    val padron_txt = spark.read.format("csv") // Format of dataset
      .option("sep", ";") //Separator
      .option("header", true) //First row is a column
      .option("inferSchema", "true") //infering schema
      .load(padronPath) //Load from path. could be an hdfs path

    //Deleted whitespace in strings columns starting by 'DESC' with trim
    padron_txt.show(10)
    val padron_trim = padron_txt.select(padron_txt.columns.map(x => if( x.startsWith("DESC"))
      {
        trim(col(x)).as(x)
      } else {
        col(x)
    }): _*)

    /* Alternative deleting whitespaces using trim
    val padron_txt2 = padron_txt.withColumn("DESC_DISTRITO", trim(padron_txt("DESC_DISTRITO")))
    val padron_txt3 = padron_txt2.withColumn("DESC_BARRIO", trim(padron_txt2("DESC_BARRIO")))
    //padron_txt3.show(10, false)
    */

    //Delete null rows from differents columns
    val padron = padron_trim.na.fill((0), Seq("EspanolesHombres", "EspanolesMujeres", "ExtranjerosHombres", "ExtranjerosMujeres"))

    //Total Men, women and foreign men and women group by Desc_Dist and Desc_barrio
    val padronSum =  padron.groupBy(col("DESC_DISTRITO"), col("DESC_BARRIO")).sum("EspanolesHombres","EspanolesMujeres","ExtranjerosHombres","ExtranjerosMujeres")
    padronSum.show(10)

    //Total people in each "Desc_distrito"
    padronSum.select(padronSum("DESC_DISTRITO"), (padronSum("sum(EspanolesHombres)") + padronSum("sum(EspanolesMujeres)") + padronSum("sum(ExtranjerosHombres)") + padronSum("sum(ExtranjerosMujeres)")).alias("total"))
      .sort(asc("total"))
      .show(30)
    
    //Saving padron dataset in Parquet format.
    val parquetPath="./FinalDataset"
    padron.write.format("parquet").save(parquetPath)
  }
}
