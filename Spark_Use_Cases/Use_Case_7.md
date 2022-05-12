# Spark use case 7

Mauro Travieso

---

**Requirements:**

  * Find out all ***PENDING_PAYMENT*** orders in ***March 2014***.
  * ***order_date*** format is *unix_timestamp*.
  * Input file is ***Parquet*** format stored at HDFS location: ***/user/spark/dataset/q7***  

**Output requirements:**

  * Result shold be saved in HDFS at location: ***/user/spark/dataset/q7/output***.
  * Output should be ***order_date*** and ***total pending orders for that date***.
  * Output should be ***JSON*** file format.
  * Output file should be saved in 4 files.

**Sample Output:**
```json
  {
    "order_date":"2014-03-12","pending_orders":10,
    "order_date":"2014-03-13","pending_orders":15,
    ...
    ...
    ...
  }
```

**Implementation:**

* Performing EDA over the parquet files:

```scala
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

* Transforming timestamp to HRF (human readable format)

```scala
+--------+----------+-----------------+---------------+
|order_id|order_date|order_customer_id|   order_status|
+--------+----------+-----------------+---------------+
|   25003|2013-12-25|             6045|        PENDING|
|   25004|2013-12-25|             5043|       COMPLETE|
|   25005|2013-12-25|             7594|       COMPLETE|
|   25006|2013-12-25|             4425|SUSPECTED_FRAUD|
|   25007|2013-12-25|             5603|         CLOSED|
+--------+----------+-----------------+---------------+
```

* Selecting **order_date** as *Mar 2014* and **order_status** equal to *PENDING_PAYMENT*, and performing aggregation operations

```scala
+----------+--------------+                                                     
|order_date|pending_orders|
+----------+--------------+
|2014-03-21|            48|
|2014-03-05|            50|
|2014-03-09|            64|
|2014-03-02|            62|
|2014-03-30|            67|
+----------+--------------+
only showing top 5 rows

```

* Storing the data in HDFS as JSON format:
```
$ hdfs dfs -cat /user/spark/dataset/q7/output/part*

{"order_date":"2014-03-21","pending_orders":48}
{"order_date":"2014-03-05","pending_orders":50}
{"order_date":"2014-03-09","pending_orders":64}
{"order_date":"2014-03-02","pending_orders":62}
{"order_date":"2014-03-30","pending_orders":67}
{"order_date":"2014-03-19","pending_orders":34}
{"order_date":"2014-03-16","pending_orders":19}
```

* Fetching the data from HDFS into Spark Shell

```scala
scala> val res = (spark
.read
.format("json")
.load("hdfs://localhost:9000/user/spark/dataset/q7/output"))

scala> res.show(5)
+----------+--------------+
|order_date|pending_orders|
+----------+--------------+
|2014-03-01|            27|
|2014-03-25|            61|
|2014-03-06|            34|
|2014-03-28|            18|
|2014-03-10|            29|
+----------+--------------+
only showing top 5 rows
```

* Performing sort on pending_orders
```
+----------+--------------+
|order_date|pending_orders|
+----------+--------------+
|2014-03-30|            67|
|2014-03-09|            64|
|2014-03-02|            62|
|2014-03-25|            61|
|2014-03-29|            57|
+----------+--------------+
only showing top 5 rows
```
