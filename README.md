## SparkHive

1. 使用maven将程序打包成jar

	`mvn package`
 	
2. 使用scp命令将jar上传至集群中

	`scp -P port /path_to_jar/SparkHive-1.0-SNAPSHOT.jar root@n141`

3. 在集群中使用spark-submit命令提交任务

	```
	spark-submit --class com.wdongyu.hive.SparkHive \
                 --master spark://hadoop-spark-master:7077 \
                 --conf spark.driver.extraJavaOptions="-Dfile.encoding=utf-8" \
                 --conf spark.executor.extraJavaOptions="-Dfile.encoding=utf-8" \
                 --executor-memory 4G \
                 SparkHive-1.0-SNAPSHOT.jar
	```