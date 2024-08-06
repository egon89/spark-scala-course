package com.egon.sparkvideocourse

import org.apache.spark.sql.functions.{col, current_timestamp, expr}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object Main {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("spark-video-course")
      .master("local[*]")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    val dataFrame: DataFrame = sparkSession.read
      .option("header", value = true)
      .option("inferSchema", value = true)
      .csv("data/aapl.csv")

    // showing top 20 rows with header
    dataFrame.show()

    dataFrame.printSchema()

    // show selected columns
    dataFrame.select("Date", "Open", "Close").show(10)

    // referencing columns
    val dateColumn: Column = dataFrame("Date")
    val openColumn: Column = col("Open")
    dataFrame.select(dateColumn, openColumn).show(5)

    println("> Column functions")
    val columnIncreasedByTwo = (openColumn + 2.0).as("columnIncreasedByTwo")
    val columnIncreasedByOne = openColumn.plus(1).as("columnIncreasedByOne")
    val columnMultipliedByOne = (openColumn * 1).as("columnMultipliedByOne")
    val castColumnToString = openColumn.cast(StringType).substr(0, 4).as("stringValue")
    dataFrame.select(
        openColumn, columnMultipliedByOne, columnIncreasedByOne, columnIncreasedByTwo, castColumnToString)
      .filter(columnIncreasedByTwo > 4)
      .filter(openColumn === columnMultipliedByOne)
      .show()

    println("> SQL expressions")
    // show a data frame with 5 lines of the current timestamp as string
    val timestampFromExpression = expr("cast(current_timestamp() as string) as timestampExpression") // any spelling mistake will throw error only in program execution
    val timestampFromFunction = current_timestamp().cast(StringType).as("timestampFunction")
    dataFrame.select(timestampFromExpression, timestampFromFunction).show(5)

    // we can select columns using sql expressions
    // the Open + 1.0 means the column Open with values higher than 1.0
    dataFrame.selectExpr("cast(Date as string)", "Open + 1.0", "current_timestamp()").show()

    // we can use our data frame as a table, but for the spark session recognizes the data frame,
    // we could register it as a temp view
    dataFrame.createTempView("df")
    sparkSession.sql("select * from df").show()

    println("> Rename columns name, add custom column and filter")
    val renameColumns = List(
      col("Date").as("date"),
      col("Open").as("open"),
      col("High").as("high"),
      col("Low").as("low"),
      col("Close").as("close"),
      col("Adj Close").as("adjClose"),
      col("Volume").as("volume"),
    )
    // the _* means varargs
    val stockData = dataFrame.select(renameColumns: _*)
      .withColumn("diff", col("close") - col("open"))
      .filter(col("close") > col("open") * 1.1) // stock that grew 10% in relation to the open value

    stockData.show()
  }
}
