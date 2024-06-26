# Apache Spark and Scala

- Spark Session is the entry point for the "Spark world" in the Scala application
```scala
SparkSession.builder()
      .appName("spark-video-course")
      .master("local[*]")
      .getOrCreate()
```
- the `*` means that the Spark will create a number of executors according the available core numbers 

### Add VM Options (Intellij)
`--add-exports java.base/sun.nio.ch=ALL-UNNAMED`

## Data Frame

### show
![show](docs/images/data-frame-show.png)

### printSchema
- [SQL data types reference](https://spark.apache.org/docs/latest/sql-ref-datatypes.html)
- [CSV data source](https://spark.apache.org/docs/latest/sql-data-sources-csv.html)

All columns are recognized as string because the `inferSchema` default value is false  

![show](docs/images/data-frame-schema.png)

To infers the input schema automatically from data, we need to set the `inferSchema` as true

![show](docs/images/data-frame-infer-schema.png)

> If at least one value of the column has a different type, all column will be a string

---
Reference: [Apache Spark & Scala Course](https://www.youtube.com/watch?v=l9vuR2SPGGQ&list=PLeEh_6coH9EpIwsiNiDCW0V-UjNZElF0d)
