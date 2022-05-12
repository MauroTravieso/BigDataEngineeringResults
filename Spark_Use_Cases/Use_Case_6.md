# Spark use case 6

Mauro Travieso

---

**Instructions:**

  * Get customers from metastore table named ***"customers_hive"*** who lives in ***'TX'*** state & first name starts with ***'M'***

**Output requirements:**

  * Result should be saved in ***/user/spark/dataset/q6/output***.
  * Output should be saved as ***parquet*** file.
  * Files should be saved in ***uncompressed*** format.

**Implementation:**

* Performing EDA from Hive

```scala
hive> SELECT * FROM customers_hive LIMIT 5;
OK
1	Richard	Hernandez	XXXXXXXXX	XXXXXXXXX	6303 Heather Plaza	BrownsvilleTX	78521
2	Mary	Barrett	XXXXXXXXX	XXXXXXXXX	9526 Noble Embers Ridge	Littleton	CO	80126
3	Ann	Smith	XXXXXXXXX	XXXXXXXXX	3422 Blue Pioneer Bend	Caguas	PR	00725
4	Mary	Jones	XXXXXXXXX	XXXXXXXXX	8324 Little Common	San Marcos	CA	92069
5	Robert	Hudson	XXXXXXXXX	XXXXXXXXX	10 Crystal River Mall 	Caguas	PR	00725
Time taken: 0.334 seconds, Fetched: 5 row(s)
```

```scala
hive> SELECT COUNT(*) FROM customers_hive;
Total MapReduce CPU Time Spent: 5 seconds 990 msec
OK
```
```
12435
```
```
Time taken: 31.653 seconds, Fetched: 1 row(s)
```

* Performing the data exploration from spark RPL

```scala
scala> spark.sql("SELECT * FROM customers_hive WHERE customer_state='TX' AND customer_fname LIKE 'M%'").show(5))

+-----------+--------------+--------------+--------------+--------------+--------------------+-------------+--------------+------------+
|customer_id|customer_fname|customer_lname|customer_email|customer_passw|    customer_address|customer_city|customer_state|customer_zip|
+-----------+--------------+--------------+--------------+--------------+--------------------+-------------+--------------+------------+
|         29|          Mary|      Humphrey|     XXXXXXXXX|     XXXXXXXXX|2469 Blue Brook C...|   Fort Worth|            TX|       76133|
|        239|          Mary|       Simpson|     XXXXXXXXX|     XXXXXXXXX|5530 Dewy Pioneer...|      Mcallen|            TX|       78501|
|        252|          Mary|       Frazier|     XXXXXXXXX|     XXXXXXXXX|7126 Shady Bluff ...|   Richardson|            TX|       75080|
|        274|          Mary|         Moyer|     XXXXXXXXX|     XXXXXXXXX|5120 Indian Pine ...|  San Antonio|            TX|       78240|
|        300|          Mary|           Key|     XXXXXXXXX|     XXXXXXXXX|6630 Umber Dale View|        Plano|            TX|       75093|
+-----------+--------------+--------------+--------------+--------------+--------------------+-------------+--------------+------------+
only showing top 5 rows

```

* Fetching the result table form HDFS:

```scala
scala> val res = (spark
     | .read
     | .load("hdfs://localhost:9000/user/spark/dataset/q6/output"))
res: org.apache.spark.sql.DataFrame = [customer_id: int, customer_fname: string ... 7 more fields]

scala> res.show(5)
+-----------+--------------+--------------+--------------+--------------+--------------------+-------------+--------------+------------+
|customer_id|customer_fname|customer_lname|customer_email|customer_passw|    customer_address|customer_city|customer_state|customer_zip|
+-----------+--------------+--------------+--------------+--------------+--------------------+-------------+--------------+------------+
|         29|          Mary|      Humphrey|     XXXXXXXXX|     XXXXXXXXX|2469 Blue Brook C...|   Fort Worth|            TX|       76133|
|        239|          Mary|       Simpson|     XXXXXXXXX|     XXXXXXXXX|5530 Dewy Pioneer...|      Mcallen|            TX|       78501|
|        252|          Mary|       Frazier|     XXXXXXXXX|     XXXXXXXXX|7126 Shady Bluff ...|   Richardson|            TX|       75080|
|        274|          Mary|         Moyer|     XXXXXXXXX|     XXXXXXXXX|5120 Indian Pine ...|  San Antonio|            TX|       78240|
|        300|          Mary|           Key|     XXXXXXXXX|     XXXXXXXXX|6630 Umber Dale View|        Plano|            TX|       75093|
+-----------+--------------+--------------+--------------+--------------+--------------------+-------------+--------------+------------+
only showing top 5 rows
```

