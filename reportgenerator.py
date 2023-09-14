from configparser import ConfigParser

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date

def main():
 spark = SparkSession.builder.config("spark.jars", "C:\installers\Drivers\postgresql-42.6.0.jar").appName("jdbc").master("local").getOrCreate()

 config = ConfigParser()

 config_path = "C:/Users/MUDVARMA/PycharmProjects/pythonProject1/File.properties"

 with open(config_path, "r") as config_file:

  content = config_file.read()

  config.read_string(content)


 cust_path = config.get('parquet','cust_path')
 items_path =config.get('parquet','items_path')
 details_path =config.get('parquet','details_path')
 orders_path =config.get('parquet','orders_path')
 sales_path = config.get('parquet','sales_path')
 ship_path =config.get('parquet','ship_path')
 # # Read the Parquet file into a DataFrame using the spark instance
 cust_temp = spark.read.parquet(cust_path)
 # Create a temporary view from the DataFrame
 cust_temp.createOrReplaceTempView("cust_view")

 order_temp = spark.read.parquet(orders_path)
 order_temp.createOrReplaceTempView("order_view")

 items_temp = spark.read.parquet(items_path)
 items_temp.createOrReplaceTempView("items_view")

 details_temp = spark.read.parquet(details_path)
 details_temp.createOrReplaceTempView("details_view")

 sales_temp = spark.read.parquet(sales_path)
 sales_temp.createOrReplaceTempView("sales_view")

 ship_temp = spark.read.parquet(ship_path)
 ship_temp.createOrReplaceTempView("ship_view")
 properties = {
 "driver": "org.postgresql.Driver",
 "user": "postgres",
 "password": "Siv@k3567",
 "url":"jdbc:postgresql://localhost:5432/Demo"
  }
 return spark,properties
def query1(spark,properties):
 query= '''select c.cust_id,c.cust_name, count(od.ORDER_ID) as count from cust_view c  
left join order_view rd on (c.CUST_ID= rd.cust_id )
left join details_view od on ( rd.ORDER_ID = od. ORDER_ID)
left join items_view i on (od.ITEM_ID = i.ITEM_ID)
group by c.cust_id,c.cust_name;'''
 data = spark.sql(query).withColumn('current_date', current_date())
 data.show()
 data.write.mode("overwrite").jdbc(properties['url'],table='monthweek_order_count',properties=properties)
 data.coalesce(1).write.mode("overwrite").partitionBy('current_date').parquet('C:/query/MonthlyWeekly_Customer_order_count')
def query2(spark,properties):
  query2= '''select cust_id , cust_name,sum( ITEM_QUANTITY*DETAIL_UNIT_PRICE) total_order from (
select c.cust_id,c.cust_name, od.ITEM_ID, od.ITEM_QUANTITY, od.DETAIL_UNIT_PRICE  from cust_view c  
left join order_view rd on (c.CUST_ID= rd.cust_id )
left join details_view od on ( rd.ORDER_ID = od. ORDER_ID)
left join items_view i on (od.ITEM_ID = i.ITEM_ID) ) p
group by cust_id , cust_name'''
  data = spark.sql(query2).withColumn('current_date', current_date())
  data.show()
  data.write.mode("overwrite").jdbc(properties['url'],table='query2',properties=properties)
  data.coalesce(1).write.mode("overwrite").partitionBy('current_date').parquet('C:/query/sum_of_orders')
def query3(spark,properties):
  query3= '''select i.ITEM_ID,i.ITEM_DESCRIPTION , sum(ITEM_QUANTITY) as sum from items_view i
left join details_view od on (i.ITEM_ID = i.ITEM_ID)
group by i.ITEM_ID,i.ITEM_DESCRIPTION  order by sum(od.ITEM_QUANTITY) desc'''
  data = spark.sql(query3).withColumn('current_date', current_date())
  data.show()
  data.write.mode("overwrite").jdbc(properties['url'],table='item_count',properties=properties)
  data.coalesce(1).write.mode("overwrite").partitionBy('current_date').parquet('C:/query/item_wise_count')
def query4(spark,properties):
   query4= '''select i.CATEGORY , sum(od.ITEM_QUANTITY) as count from items_view i
left join details_view od on (i.ITEM_ID = i.ITEM_ID)
group by i.CATEGORY  order by sum(od.ITEM_QUANTITY) desc'''
   data = spark.sql(query4).withColumn('current_date', current_date())
   data.show()
   data.write.mode("overwrite").jdbc(properties['url'],table='Item_descending',properties=properties)
   data.coalesce(1).write.mode("overwrite").partitionBy('current_date').parquet('C:/query/Item_descending')

# def query5(spark,properties):
#    query5= '''select i.ITEM_ID,i.ITEM_DESCRIPTION , sum(i.ITEM_QUANTITY * od.DETAIL_UNIT_PRICE) as count from item_view i
# left join details_view od on (i.ITEM_ID = i.ITEM_ID)
# group by i.ITEM_ID,i.ITEM_DESCRIPTION  order by sum(od.ITEM_QUANTITY * od.DETAIL_UNIT_PRICE) desc'''
#    data = spark.sql(query5).withColumn('current_date', current_date())
#    data.show()
#    data.write.mode("overwrite").jdbc(properties['url'],table='query5',properties=properties)
#    data.coalesce(1).write.mode("overwrite").partitionBy('current_date').parquet('C:/query/query5')
# #wire.write.jdbc(properties['url'], table='temp_customers', properties=properties)
# wire.write.parquet('C:/query/temp_cust')
#     # Display the schema of the DataFrame
# #parquet_df.printSchema()
#
#     # Show the first few rows of the DataFrame
# #parquet_df.show()



if __name__ == "__main__":
    spark,properties=main()
    query1(spark,properties)
    query2(spark,properties)
    query3(spark,properties)
    query4(spark,properties)
    #query5(spark,properties)
