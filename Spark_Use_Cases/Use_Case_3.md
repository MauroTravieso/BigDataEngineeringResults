# Spark use case 3

Mauro Travieso

---

**Instructions:**

  * From provided Parquet files located at below HDFS location, get ***maximun product_price*** in each ***product_category*** from:

    ***/user/spark/dataset/mock1/q3*** dataset.

**Output requirements:**

  * Result should be saved in ***/user/spark/dataset/q3/output***.
  * Final output should have ***product_id*** and ***max_price*** separated by *pipe* delimiter.
  * Output should be saved in ***text format*** with ***Gzip*** compression
  * Order the results by maximum price ***descending***
  
 **Implementation:**
 
```
+----------+-------------------+--------------------+-------------------+-------------+--------------------+
|product_id|product_category_id|        product_name|product_description|product_price|       product_image|
+----------+-------------------+--------------------+-------------------+-------------+--------------------+
|       673|                 31|PING G30 Fairway ...|                   |        249.0|http://images.acm...|
|       674|                 31|     PING G30 Hybrid|                   |        219.0|http://images.acm...|
|       675|                 31|PING G30 Irons - ...|                   |        799.0|http://images.acm...|
|       676|                 31|PING G30 Irons - ...|                   |        899.0|http://images.acm...|
|       677|                 31|TaylorMade White ...|                   |        99.99|http://images.acm...|
+----------+-------------------+--------------------+-------------------+-------------+--------------------+
only showing top 5 rows
```

```
scala> res.show()
+----------+
|     value|
+----------+
| 44|399.99|
|  6|399.99|
| 41|399.99|
| 17|399.99|
|  8|399.99|
| 49|399.99|
| 53|199.99|
| 13|199.99|
|  3|199.99|
| 39|199.99|
| 29|199.99|
| 54|299.99|
| 35|299.99|
|  2|299.99|
| 57|189.99|
| 19|189.99|
| 18|189.99|
|22|1799.99|
| 4|1799.99|
| 48|799.99|
+----------+
only showing top 20 rows
```
 
 
