# Spark use case 3

Mauro Travieso

---

**Instructions:**

  * Convert avro data-files at below HDFS location into tab delimited file.
  * Input folder is: ***/user/spark/dataset/q5***

**Output requirements:**

  * Result should be saved in HDFS at location ***/user/spark/dataset/q5/output***
  * Output file should be saved as ***tab delimited file*** in ***bzip2*** Compression
  * Output file should consist on:
```
   customer_id, customer_name (only first three letters), customer_lname
```

**Sample Output:**
```
  21 And Smith
  111 Mar Jonson
```

**Implementation:**

```
+-----------+--------------+--------------+
|customer_id|customer_fname|customer_lname|
+-----------+--------------+--------------+
|       9327|         Donna|         Smith|
|       9328|          Mary|         Perez|
|       9329|        Eugene|        Powell|
|       9330|          Mary|        Conley|
|       9331|         Donna|         Smith|
+-----------+--------------+--------------+
only showing top 5 rows

```

```
$ hdfs dfs -ls /user/spark/dataset/q5/output
Found 5 items
-rw-r--r--   3 ubuntu supergroup          0 2022-05-04 06:19 /user/spark/dataset/mock1/q5/output/_SUCCESS
-rw-r--r--   3 ubuntu supergroup      27585 2022-05-04 06:18 /user/spark/dataset/mock1/q5/output/part-00000-c8d10d06-7920-4a52-a05d-0186672f6d04-c000.snappy.parquet
-rw-r--r--   3 ubuntu supergroup      27608 2022-05-04 06:18 /user/spark/dataset/mock1/q5/output/part-00001-c8d10d06-7920-4a52-a05d-0186672f6d04-c000.snappy.parquet
-rw-r--r--   3 ubuntu supergroup      27583 2022-05-04 06:18 /user/spark/dataset/mock1/q5/output/part-00002-c8d10d06-7920-4a52-a05d-0186672f6d04-c000.snappy.parquet
-rw-r--r--   3 ubuntu supergroup      27508 2022-05-04 06:18 /user/spark/dataset/mock1/q5/output/part-00003-c8d10d06-7920-4a52-a05d-0186672f6d04-c000.snappy.parquet
```

```
+-----------+--------------+--------------+
|customer_id|customer_fname|customer_lname|
+-----------+--------------+--------------+
|       9327|           Don|         Smith|
|       9328|           Mar|         Perez|
|       9329|           Eug|        Powell|
|       9330|           Mar|        Conley|
|       9331|           Don|         Smith|
+-----------+--------------+--------------+
only showing top 5 rows
```
