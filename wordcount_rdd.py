from  pyspark.sql import SparkSession
import re, time

spark=SparkSession.builder.appName("wordCountRD").getOrcreate()
sc=spark.sparkcontext
sc.setLogLEVEL("ERROR")

start=time.time()
lines=sc.textFile("file:///home/ubuntu/mnt/d/wordcount_project/scripts")

word_re=re.compile(r"[A-Za-Z']+")
words=lines.flatMap(lambda line: word_re.findall(line.lower()))

counts=words.map(lambda w: (w, 1)).reduceByKey(lambda a, b:a+b)

output_path=""

hadoop=sc._jvm.org.apache.hadoop

fs=hadoop.fs.FileSystem.get(sc.jsc.hadoopConfiguration())
path=hadoop.fs.path(output_path)
if fs.exists(path):
    fs.delete(path,True)
counts.saveAsTextFile(output_path)

elapsed=time.time()-start

print(f"Spark RDD wordcount finished in {elapsed:.2f} seconds")

spark.stop()
