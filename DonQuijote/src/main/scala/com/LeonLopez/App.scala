package com.LeonLopez

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
 * @author ${user.name}
 * @description Using famous Don Quixote book like dataset we explore functions like show, count, head, take and first
 *              and their differences
 * Dataset URL: https://gist.github.com/jsdario/6d6c69398cb0c73111e49f1218960f79
 */
object App {

  def main(args : Array[String]): Unit = {

    //Create Spark session
    val spark: SparkSession = SparkSession.builder() //Create SparkSession
      .master("local[1]") //Number of cores
      .appName("DonQuixote") //App Name
      .getOrCreate()

    //Stop Log messages
    Logger.getLogger("org.apache").setLevel(Level.ERROR)
    Logger.getLogger("org.spark-project").setLevel(Level.ERROR)

    //Loading dataset
    val path = "el_quijote.txt"
    val quixote = spark.read
      .textFile(path)

    //Show by default - Truncate false and 20 rows by default
    quixote.show()

    //Not truncate show
    quixote.show(false)

    //Num of lines
    println("Num lines in the book: " + quixote.count())

    //limit num rows 20 by default
    quixote.show(20)
    quixote.show(20, false)

    //HEAD FUNCTION
    //Returns first n rows of DataFrame. An String object if n=1. String array if n>1
    val quixoteHead = quixote.head(15) //String Array
    println("\n\n-----HEAD FUNCTION-----\n"+ quixoteHead.getClass)
    println(quixoteHead(1)) //Using index to show
    println(quixoteHead(4))

    //TAKE FUNCTION. An String object if n=1. String array if n>1
    //Take n rows from dataframe
    val quixote15lines = quixote.take(15)
    println("\n\n-----TAKE FUNCTION-----\n" + quixote15lines.getClass) //String Array
    println(quixote15lines(0)) //Using index to show
    println(quixote15lines(14))

    //FIRST FUNCTION. Always give us just the first row
    val quixoteFirst = quixote.first()
    println("\n\n-----FIRST FUNCTION-----\n" + quixoteFirst.getClass)
    println(quixoteFirst)

    /**
     *  DIFFERENCE BETWEEN TAKE, HEAD AND FIRST
     *  The functions do the same but can be referenced in different ways, which is useful for people who are new
     *  with the language and who are coming from other technologies. Something similar happens with the filter()
     *  function and the where() function.
     *
     */
  }

}
