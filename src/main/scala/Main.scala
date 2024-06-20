package com.egon.sparkvideocourse

import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("spark-video-course")
      .master("local[*]")
      .getOrCreate()

    val dataFrame = sparkSession.read
      .csv("data/aapl.csv")

    dataFrame.show()
  }
}
