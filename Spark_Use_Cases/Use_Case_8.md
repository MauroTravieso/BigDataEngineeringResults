# Spark use case 8

Mauro Travieso

---

**Instructions:**

  * Find out the total number of orders placed by each customer in the year 2013.
  * Order status should be ***COMPLETE***.
  * ***order_date*** format is in ***unix_timestamp***.
  * Input customer & order files are stored as avro file at below HDFS locations:

    ***/user/spark/dataset/q8/orders***

    ***/user/spark/dataset/q8/customers***

**Output requirements:**

  * Output should be stored in a hive table named ***"customer_order"***.
  * Hive table should have three columns:

    ***customer_fname***, ***customer_lname*** and ***orders_count***.

  * Hive tables should be partitioned by ***customer_state***.

**Sample output:**
```
  21 And Smith
  111 Mar Jons
```

**Implementation:**

```scala
val customers = (spark
.read
.format("avro")
.load("hdfs://localhost:9000/user/spark/dataset/q8/customers")
.show(5))
```

* Customer Data
```scala
+-----------+--------------+--------------+--------------+-----------------+--------------------+-------------+--------------+----------------+
|customer_id|customer_fname|customer_lname|customer_email|customer_password|     customer_street|customer_city|customer_state|customer_zipcode|
+-----------+--------------+--------------+--------------+-----------------+--------------------+-------------+--------------+----------------+
|       9327|         Donna|         Smith|     XXXXXXXXX|        XXXXXXXXX|4114 Clear Nectar...|       Caguas|            PR|           00725|
|       9328|          Mary|         Perez|     XXXXXXXXX|        XXXXXXXXX|  376 Golden Orchard|Moreno Valley|            CA|           92553|
|       9329|        Eugene|        Powell|     XXXXXXXXX|        XXXXXXXXX|   2161 Burning Maze|     Metairie|            LA|           70003|
|       9330|          Mary|        Conley|     XXXXXXXXX|        XXXXXXXXX| 3046 Broad Sky Dale|       Caguas|            PR|           00725|
|       9331|         Donna|         Smith|     XXXXXXXXX|        XXXXXXXXX|941 Thunder Branc...|    Clementon|            NJ|           08021|
+-----------+--------------+--------------+--------------+-----------------+--------------------+-------------+--------------+----------------+
only showing top 5 rows
```

* Orders Data:

```scala
scala> orders.show(5)
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

```scala
hive> DESCRIBE customer_order;
OK
customer_fname      	string              	                    
customer_lname      	string              	                    
orders_count        	bigint              	                    
customer_state      	string
```

```scala
hive> SELECT * FROM customer_order LIMIT 5;
OK
Scott	Stone	2	AR
Christian	Smith	1	AR
Sean	Smith	1	AR
Mary	Butler	2	AR
Joyce	Smith	2	AZ
Time taken: 1.077 seconds, Fetched: 5 row(s)
```

```scala
scala> res.show(5)
+--------------+--------------+------------+--------------+                     
|customer_fname|customer_lname|orders_count|customer_state|
+--------------+--------------+------------+--------------+
|       Douglas|         Keith|           2|            MD|
|          Mary|       Goodwin|           3|            MD|
|          Mary|          Moon|           1|            MD|
|          Mary|          Bass|           1|            MD|
|          Mary|       Shaffer|           1|            MD|
+--------------+--------------+------------+--------------+
only showing top 5 rows
```

```scala
scala> val res = spark.sql("SELECT COUNT(*) FROM customer_order WHERE customer_state = 'TX'")
res: org.apache.spark.sql.DataFrame = [count(1): bigint]

scala> res.show()
+--------+                                                                      
|count(1)|
+--------+
|     344|
+--------+
```




