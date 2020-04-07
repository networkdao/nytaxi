# 纽约出租车行程数据分析
![Image text](https://github.com/networkdao/nytaxi/blob/master/image/nytaxi1.png)
## 分析的目的
1. 通过分析发现工作时间最长的司机们 
2. 司机行驶的里程数，行驶时间（判断是否疲劳驾驶）  
3. 四驱租出车和非四驱的出租车的行程上是否有差异 
4. 根据位置信息作出热力图
（部分进行中） 

## 数据来源
https://www1.nyc.gov/site/tlc/about/data-and-research.page


## 数据格式和规模
1. 单个原始文件>2GB， 单个文件超过1400W 行；
总共12个文件。

## 代码部分
job1totaltimeTop20.py--   2013年1月累计载客时间Top20 的司机  
job2_1_201301status.py -- 2013年1月司机的该工作状态（根据工作时间定义的工作状态）  
job2_2_201301statusnopw.py -- 2013年1月司机中工作状态的具体信息写入到Mysql 数据库，方便查询  

## 使用的大数据平台或框架
- Spark
- HDFS
- Hive
- ElasticSearch
- Kibana
- Azkaban
...


