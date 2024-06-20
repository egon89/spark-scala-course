# Apache Spark and Scala

- Spark Session is the entry point for the "Spark world" in the Scala application
```scala
SparkSession.builder()
      .appName("spark-video-course")
      .master("local[*]")
      .getOrCreate()
```
- the `*` means that the Spark will create a number of executors according the available core numbers 


---
Reference: [Apache Spark & Scala Course](https://www.youtube.com/watch?v=l9vuR2SPGGQ&list=PLeEh_6coH9EpIwsiNiDCW0V-UjNZElF0d)
