package com.alvinxu.rdd

import org.apache.spark.{SparkConf, SparkContext}

object RDDapi {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("rddtest1")
    /*conf.setMaster("spark://172.16.18.131:18088")*/
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rddInt = sc.makeRDD(Array(1, 3, 4, 6))
    val rddIntUnion = sc.makeRDD(Array(2, 3, 6, 9))
    val rddStr = sc.makeRDD(Array("alvinxu", "xu", "fei", "xu"))
    println(rddStr)

    /*
    *转换操作transfer
    * */
    /*map操作*/
    val rddMap = rddInt.map(x => x + 1).collect().mkString("//")
    println(rddMap)
    /*filter操作*/
    val rddFilter = rddInt.map(x => x >= 4).collect().mkString(",")
    println(rddFilter)
    /*flatMap操作*/
    val rddFlatMap = rddStr.flatMap(x => x.split(",")).collect().mkString("~")
    println(rddFlatMap)
    /*distant操作（去重）*/
    val rddDistant = rddStr.distinct().collect().mkString("~")
    println(rddDistant)
    /*union操作*/
    val rddUnion = rddInt.union(rddIntUnion).collect().mkString("_")
    println(rddUnion)
    /*intersection操作*/
    val rddIntersection = rddInt.intersection(rddIntUnion).collect().mkString(",")
    println(rddIntersection)
    /*subtract操作，去掉rddInt中在rddIntUnion中存在的元素*/
    val rddSubtract = rddInt.subtract(rddIntUnion).collect().mkString(",")
    println(rddSubtract)

    /*
    *动作action操作
    * */
    /*count操作*/
    val rddCount = rddInt.count()
    println(rddCount)
    /*countByVule*/
    val rddCountByValue = rddInt.countByValue()
    println(rddCountByValue)
    /*reduce操作，前两个数进行运算后作为新返回值参与下一次运算*/
    val rddReduce = rddInt.reduce((x,y)=>x+y)
    println(rddReduce)
    /*reduceByKey操作*/
    val rddKV = sc.parallelize(List((1,3),(1,4),(2,5)))
    val rddReduceByKey = rddKV.reduceByKey((x,y)=>x+y).collect().mkString(",")
    println(rddReduceByKey)
  }
}
