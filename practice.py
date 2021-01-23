from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark import SparkConf
import os
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import *
from pyspark.sql import functions as F

sc = SparkContext(master="local[*]",appName="sparkrdd")
spark = SparkSession.builder\
    .config("spark.sql.autoBroadcastJoinThreshold", -1) \
    .config("spark.executor.memory", "500mb")\
     .appName("Exercise1") \
     .getOrCreate()
os.system("pwd")

products_table = spark.read.option("mode","dropmalformed").parquet("/Users/nandinipatnaik/Downloads/DatasetToCompleteTheSixSparkExercises/products_parquet")
sales_table = spark.read.option("mode","dropmalformed").parquet("/Users/nandinipatnaik/Downloads/DatasetToCompleteTheSixSparkExercises/sales_parquet")
sellers_table = spark.read.option("header","true").csv("/Users/nandinipatnaik/Downloads/DatasetToCompleteTheSixSparkExercises/sellers.csv")
print(products_table)
products_table.show()
sellers_table.show()
sales_table.show()
sales_table.where(F.col("order_id")=="477821").show()
#Print the number of orders
print("Number of Orders: {}".format(sales_table.count()))
#Print the number of sellers
print("Number of sellers: {}".format(sellers_table.count()))
#Print the number of products
print("Number of products: {}".format(products_table.count()))
#How many products have been sold at least once? Which is the product contained in more orders?
sales_table.groupBy("product_id").agg(F.count("*").alias("cnt")).orderBy(F.col("cnt").desc()).limit(1).show()
products_table.createOrReplaceTempView("product_data")
#spark.sql("select * from product_data").show()
sales_table.createOrReplaceTempView("sales_data")
sellers_table.createOrReplaceTempView("sellers_data")
spark.sql("select product_id,count('*') as cnt from sales_data group by product_id order by cnt DESC").show()
#how many distinct products have been sold in each date
spark.sql("select date,count(distinct product_id) as cnt from sales_data group by date").show()
sales_table.groupBy("date").agg(F.countDistinct("product_id")).show()
#What is the average revenue of the orders?”
spark.sql("select avg(p.price * o.num_pieces_sold) as revenue, o.order_id from sales_data as o join product_data as p on o.product_id = p.product_id group by order_id").show()
sales_table.join(products_table,sales_table.product_id==products_table.product_id,"inner").agg(F.avg((products_table.price)*(sales_table.num_pieces_sold))).show()
sales_table.join(broadcast(products_table),sales_table.product_id==products_table.product_id,"inner").explain()
sales_table.join((products_table),sales_table.product_id==products_table.product_id,"inner").explain()