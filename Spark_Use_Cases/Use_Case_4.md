# Spark use case 4

Mauro Travieso

---

**Instructions:**

  * Provided customer tab delimited files at below HDFS location, find all customers that live in ***'Caguas'*** city.

  * Input folder is: ***/user/spark/dataset/q4***

**Output schema should be:**
```
  customer_id:Integer, customer_name:String, customer_city:String
```
**Output requirements:**

  * Results should be saved in: ***/user/spark/dataset/mock1/q4/output***.
  * Output file should be saved in ***avro*** format.
  * Output should be saved with ***deflate*** compression.

**Implementation:**

```scala
root
 |-- customer_id: integer (nullable = true)
 |-- customer_name: string (nullable = true)
 |-- customer_city: string (nullable = true)
```

```scala
scala> df.show(5)
+-----------+-------------+-------------+
|customer_id|customer_name|customer_city|
+-----------+-------------+-------------+
|       9327|        Donna|       Caguas|
|       9330|         Mary|       Caguas|
|       9332|         Mary|       Caguas|
|       9335|       Joseph|       Caguas|
|       9339|          Ann|       Caguas|
+-----------+-------------+-------------+
only showing top 5 rows
```

```scala
scala> df.count()
res30: Long = 4584
```

```scala
$ hdfs dfs -ls /user/spark/dataset/q4/output
Found 5 items
-rw-r--r--   3 ubuntu supergroup          0 2022-05-04 05:51 /user/spark/dataset/mock1/q4/output/_SUCCESS
-rw-r--r--   3 ubuntu supergroup       5324 2022-05-04 05:51 /user/spark/dataset/mock1/q4/output/part-00000-85505435-1cb6-4051-9298-7d67d1296726-c000.avro
-rw-r--r--   3 ubuntu supergroup       5252 2022-05-04 05:51 /user/spark/dataset/mock1/q4/output/part-00001-85505435-1cb6-4051-9298-7d67d1296726-c000.avro
-rw-r--r--   3 ubuntu supergroup       5283 2022-05-04 05:51 /user/spark/dataset/mock1/q4/output/part-00002-85505435-1cb6-4051-9298-7d67d1296726-c000.avro
-rw-r--r--   3 ubuntu supergroup       5329 2022-05-04 05:51 /user/spark/dataset/mock1/q4/output/part-00003-85505435-1cb6-4051-9298-7d67d1296726-c000.avro
```

```scala
scala> res.show(5)
+-----------+-------------+-------------+
|customer_id|customer_name|customer_city|
+-----------+-------------+-------------+
|          3|          Ann|       Caguas|
|          5|       Robert|       Caguas|
|          7|      Melissa|       Caguas|
|          9|         Mary|       Caguas|
|         11|         Mary|       Caguas|
+-----------+-------------+-------------+
only showing top 5 rows
```

```scala
scala> res.printSchema
root
 |-- customer_id: integer (nullable = true)
 |-- customer_name: string (nullable = true)
 |-- customer_city: string (nullable = true)
```

```scala
scala> res.count
res35: Long = 4584
```

