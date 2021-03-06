from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
if __name__ == '__main__':
    spark = SparkSession.builder.appName("nytaxi201301totaltime").getOrCreate()
    #从HDFS 加载CSV 文件， 并且使用现有csv文件的表头，自动推导字段的属性， 并且选择（select）后的内容
    data201301 = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/data/hdfsfile/taxiny/\
    trip_data_1.csv").select("medallion","hack_license","vendor_id","rate_code","store_and_fwd_flag","pickup_datetime",\
    "dropoff_datetime","passenger_count","trip_time_in_secs","trip_distance","pickup_longitude","pickup_latitude",\
    "dropoff_longitude","dropoff_latitude")
    #把这个data201301创建成一个虚拟表
    data201301.createOrReplaceTempView("tb201301")
    #数据清洗，去除掉行程时间小于120s 的和距离小于0.1 英里的
    sqlDF201301b3c = spark.sql("SELECT * FROM tb201301 WHERE trip_time_in_secs>=120 and trip_distance>=0.1") 
    #把清洗的数据生产一张新的虚拟表， 也可以把这个表的内容写入Hive 作为DW 的 TWD 层
    sqlDF201301b3c.createOrReplaceTempView("tbTWD201301")
    #通过spark sql 提取2013年1月份每个驾驶员的行程排行Top20累积时间，并把单位转换成小时
    sqlDF201301b3e1=spark.sql("SELECT hack_license, SUM(trip_time_in_secs)/3600 as mttimeh FROM tbTWD201301  group\
    by hack_license order by mttimeh DESC Limit 20") 
    # 把结果写入ElastiSearch
    sqlDF201301b3e1.selectExpr("hack_license","mttimeh").write.format("org.elasticsearch.spark.sql").option("es.nodes",\ 
    "172.20.224.113:9200").mode("overwrite").save("index_nytaxitotaltime/weather")
    spark.stop()
