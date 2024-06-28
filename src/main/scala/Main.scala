package com.egon.sparkvideocourse

import org.apache.spark.sql.{DataFrame, SparkSession}

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
  }
}
