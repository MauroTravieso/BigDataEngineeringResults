      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 3.0.1
      /_/
         
Using Scala version 2.12.10 (OpenJDK 64-Bit Server VM, Java 1.8.0_312)

---

**Data description:**

  * Convert Snappy compressed Avro data-files stored at HDFS location:
  ***user/spark/dataset/q1*** into Parquet file.

**Output requirements:**

  * Result should be saved in ***/user/spark/dataset/q1/output***
  * Output should consist on only ***order_id***, ***order_status***
  * Output file should be saved as ***Parquet*** file in ***gzip*** compression.

**Implementation:**

* Exploring the dataset
```scala
scala> df.show(5)
+--------+-------------+-----------------+---------------+                      
|order_id|   order_date|order_customer_id|   order_status|
+--------+-------------+-----------------+---------------+
|   25003|1388016000000|             6045|        PENDING|
|   25004|1388016000000|             5043|       COMPLETE|
|   25005|1388016000000|             7594|       COMPLETE|
|   25006|1388016000000|             4425|SUSPECTED_FRAUD|
|   25007|1388016000000|             5603|         CLOSED|
+--------+-------------+-----------------+---------------+
only showing top 5 rows
```

* Transforming the timestamp into human readable format

```scala
scala> df.show(5)
+--------+----------+-----------------+---------------+
|order_id|order_date|order_customer_id|   order_status|
+--------+----------+-----------------+---------------+
|   25003|2013-12-25|             6045|        PENDING|
|   25004|2013-12-25|             5043|       COMPLETE|
|   25005|2013-12-25|             7594|       COMPLETE|
|   25006|2013-12-25|             4425|SUSPECTED_FRAUD|
|   25007|2013-12-25|             5603|         CLOSED|
+--------+----------+-----------------+---------------+
only showing top 5 rows
```

```scala
scala> df.printSchema
root
 |-- order_id: integer (nullable = true)
 |-- order_date: date (nullable = true)
 |-- order_customer_id: integer (nullable = true)
 |-- order_status: string (nullable = true)
```
 
```scala
scala> df.count
res1: Long = 68890  
```

```
$ hdfs dfs -ls hdfs://localhost:9000/user/spark/dataset/output
Found 5 items
-rw-r--r--   3 ubuntu supergroup          0 2022-05-04 04:31 hdfs://localhost:9000/user/spark/dataset/q1/output/_SUCCESS
-rw-r--r--   3 ubuntu supergroup     116527 2022-05-04 04:31 hdfs://localhost:9000/user/spark/dataset/q1/output/part-00000-38cc6331-4949-4510-b094-eeb4fe1d893e-c000.gz.parquet
-rw-r--r--   3 ubuntu supergroup     116402 2022-05-04 04:31 hdfs://localhost:9000/user/spark/dataset/q1/output/part-00001-38cc6331-4949-4510-b094-eeb4fe1d893e-c000.gz.parquet
-rw-r--r--   3 ubuntu supergroup      95294 2022-05-04 04:31 hdfs://localhost:9000/user/spark/dataset/q1/output/part-00002-38cc6331-4949-4510-b094-eeb4fe1d893e-c000.gz.parquet
-rw-r--r--   3 ubuntu supergroup       1422 2022-05-04 04:31 hdfs://localhost:9000/user/spark/dataset/q1/output/part-00003-38cc6331-4949-4510-b094-eeb4fe1d893e-c000.gz.parquet

```
 
```
scala> val dist = res.select("order_status").distinct
dist: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [order_status: string]
```

```
scala> dist.count()
res3: Long = 9  
```
