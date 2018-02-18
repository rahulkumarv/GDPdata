import os
import sys
## Add the pyspark libraries needed - SparkContext and SparkConf
from pyspark import SparkContext, SparkConf

#Create the Spark Conf and set the application Name
#Set spark.speculation to true
if __name__ == "__main__":
        conf = SparkConf() \
        .setAppName("Spark RDD") \
        .set("spark.speculation","true")

        #Create the SparkContext from the conf
        sc = SparkContext(conf=conf)
        sc.setLogLevel("WARN")


        #Read in /user/root/selfishgiants.txt HDFS
        inputRdd = sc.textFile("/user/root/selfishgiants.txt").flatMap(lambda line: line.split(" ")).map(lambda line: (line,1))

        #Perform wordcount
        reducedRdd = inputRdd.reduceByKey(lambda a,b: a+b).map(lambda (a,b): (b,a)).sortByKey(ascending=False)

        #Print the top 10 most used words and stop the sparkcontext
        print(reducedRdd.take(10))
        sc.stop()

