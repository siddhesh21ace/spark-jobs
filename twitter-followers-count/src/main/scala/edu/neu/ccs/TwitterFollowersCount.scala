package edu.neu.ccs

import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Spark implementation for Twitter followers count program
  */
object TwitterFollowersCount {
  /** Main method */
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nwc.TwitterFollowersCountMain <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Twitter Followers Count")
    conf.set("spark.logLineage", "true")

    val sc = new SparkContext(conf)

    val listRDD = sc.textFile(args(0))
    val pairsRDD = listRDD.map(line => (line.split(",")(1), 1)) // split on ',' and pick 2nd token as a key
    val reducedRDD = pairsRDD.reduceByKey(_ + _) // reduce on the key

    println("lineage info: ", reducedRDD.toDebugString)
    reducedRDD.saveAsTextFile(args(1))
  }
}