package com.alvinxu.rdd

import org.apache.spark.{SparkConf, SparkContext}


object RddTest1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("rddtest1")
    /*conf.setMaster("spark://172.16.18.131:18088")*/
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)
    /*
    *使用makeRDD创建RDD，其中可以通过list和array创建
    * */
    val rdd1 = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9))
    val rdd2 = sc.makeRDD(Array(1, 2, 3, 4, 5))
    val rdd3 = rdd1.map(x => x * x)
    println(rdd3.collect().mkString(","))
    /*
    *使用parallelize创建
    * */
    val rdd4 = sc.parallelize(List(1,2,3,4,5,6),1)
    val rdd5 = rdd4.map(x=>x + x)
    println(rdd5.collect().mkString("-"))
  }
}
