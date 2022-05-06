# Spark use case 2

Mauro Travieso

---

**Instructions:**

  * Join the comma separated ***customers*** & ***orders*** data located at HDFS location:

    ***/user/spark/dataset/q2/orders***

    ***/user/spark/dataset/q2/customers***

    to find out customers who have placed more than 4 orders.

**Schema for *Customer* File:**
```
  customer_id, customer_fname, ...
```

**Schema for *Order* File:**
```
  order_id, order_date, order_customer_id, order_status
```

**Output requirements:**

  * ***order_status*** should be ***COMPLETE***.
  * Output should have ***customer_id***, ***customer_fname***, ***count***.
  * Result should be saved in ***/user/spark/dataset/mock1/q2/output***.
  * Save the results in ***JSON*** format.
  * Result should be ***ordered by count of orders in ascending fashion***.
  
---

**Implementation:**

```
scala> custDF.show(5)
+----+------+------+---------+---------+--------------------+-------------+---+-----+
| _c0|   _c1|   _c2|      _c3|      _c4|                 _c5|          _c6|_c7|  _c8|
+----+------+------+---------+---------+--------------------+-------------+---+-----+
|9327| Donna| Smith|XXXXXXXXX|XXXXXXXXX|4114 Clear Nectar...|       Caguas| PR|00725|
|9328|  Mary| Perez|XXXXXXXXX|XXXXXXXXX|  376 Golden Orchard|Moreno Valley| CA|92553|
|9329|Eugene|Powell|XXXXXXXXX|XXXXXXXXX|   2161 Burning Maze|     Metairie| LA|70003|
|9330|  Mary|Conley|XXXXXXXXX|XXXXXXXXX| 3046 Broad Sky Dale|       Caguas| PR|00725|
|9331| Donna| Smith|XXXXXXXXX|XXXXXXXXX|941 Thunder Branc...|    Clementon| NJ|08021|
+----+------+------+---------+---------+--------------------+-------------+---+-----+
only showing top 5 rows
```
```
scala> custDF.count()
res6: Long = 12435
```

```
scala> ordersDF.show(5)
+-----+--------------------+----+---------------+
|  _c0|                 _c1| _c2|            _c3|
+-----+--------------------+----+---------------+
|25003|2013-12-26 00:00:...|6045|        PENDING|
|25004|2013-12-26 00:00:...|5043|       COMPLETE|
|25005|2013-12-26 00:00:...|7594|       COMPLETE|
|25006|2013-12-26 00:00:...|4425|SUSPECTED_FRAUD|
|25007|2013-12-26 00:00:...|5603|         CLOSED|
+-----+--------------------+----+---------------+
only showing top 5 rows

```

```
scala> ordersDF.count()
res9: Long = 68890
```

```
scala> res.show(5)
+-----+--------------+-----------+
|count|customer_fname|customer_id|
+-----+--------------+-----------+
|    5|       Vincent|       3826|
|    5|         Smith|       7362|
|    5|        Hanson|       6248|
|    5|       Jackson|       3838|
|    5|          Diaz|       7248|
+-----+--------------+-----------+
only showing top 5 rows
```

```
scala> res.count()
res11: Long = 462
```

```
scala> res.select("customer_fname").groupBy("customer_fname").count.sort(desc("count")).show()
+--------------+-----+                                                          
|customer_fname|count|
+--------------+-----+
|         Smith|  182|
|         Perez|    4|
|       Jackson|    4|
|    Richardson|    3|
|         Jones|    3|
|      Thompson|    3|
|      Martinez|    3|
|      Robinson|    3|
|          Luna|    3|
|         Price|    3|
|         James|    2|
|       Rodgers|    2|
|         Blair|    2|
|        Hudson|    2|
|       Roberts|    2|
|            Wu|    2|
|          Mata|    2|
|       Proctor|    2|
|          Rowe|    2|
|       Marquez|    2|
+--------------+-----+
only showing top 20 rows
```
