from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
def get_status(value):
    if value <= 4 and value >= 0:
        return "悠闲"
    elif value <= 6:
        return "轻松"
    elif value <= 8:
        return "饱和"
    elif value <= 10:
        return "疲劳"
    elif value <= 12:
        return "超负荷"
    elif value >12:
        return "严重超负荷"
    else:
        return None


if __name__ == '__main__':
    spark = SparkSession.builder.appName("nytaxi201301status").getOrCreate()
    #从HDFS 加载CSV 文件， 并且使用现有csv文件的表头，自动推导字段的属性， 并且选择（select）后的内容
    data201301 = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/data/hdfsfile/taxiny/trip_data_1.csv")\
    .select("medallion","hack_license","vendor_id","rate_code","store_and_fwd_flag","pickup_datetime","dropoff_datetime","passenger_count",\
        "trip_time_in_secs","trip_distance","pickup_longitude","pickup_latitude","dropoff_longitude","dropoff_latitude")
    # 创建临时表
    data201301.createOrReplaceTempView("tb201301")
    
    #数据清洗，去除掉行程时间小于120s 的和距离小于0.1 英里的,把清洗的数据生产一张新的虚拟表， 也可以把这个表的内容写入Hive 作为DW 的 TWD 层
    sqlDF201301_0 = spark.sql("SELECT * FROM tb201301 WHERE trip_time_in_secs>=120 and trip_distance>=0.1") 
    # 创建临时表
    data201301.createOrReplaceTempView("tb201301_2")
    
    # spark sql 中的 TO_DATE 等于SQL 中的 DATE_FORMAT,qlDF201301_1 这个是中间结果1，表示的是每辆车每天拉客的累积时间,可以写入Hive
    sqlDF201301_1 = spark.sql("SELECT hack_license, TO_DATE(pickup_datetime,'%Y%m%d') AS day,sum(trip_time_in_secs) AS totaltime\
        FROM tb201301_2 GROUP BY hack_license, TO_DATE(pickup_datetime,'%Y%m%d')") 
    
    status_function_udf = udf(get_status,StringType())
    group201301_1 = sqlDF201301_1.withColumn("status", status_function_udf((sqlDF201301_1['totaltime'])/3600)).groupBy("status").count()
    group201301_2 = group201301_1.select("status","count").withColumn("percent", group201301_1['count'] / sqlDF201301_1.count()*100)

    #把最终的结果写入到elastisearch
    group201301_2.selectExpr("status","count","percent").write.format("org.elasticsearch.spark.sql").option("es.nodes", "172.20.224.113:9200").\
    mode("overwrite").save("index_nytaxistatus/job2")
    spark.stop()
