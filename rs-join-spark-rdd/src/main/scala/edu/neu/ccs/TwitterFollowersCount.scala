package edu.neu.ccs

import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Spark implementation for Twitter followers count program
  */
object TwitterFollowersCount {
  /** Main method */
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    val MAX_FILTER = 50000
    if (args.length != 2) {
      logger.error("Usage:\nwc.TwitterFollowersCountMain <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Twitter Followers Count")
    conf.set("spark.logLineage", "true")

    val sc = new SparkContext(conf)

    val lineRDD = sc.textFile(args(0))

    val filterRDD = lineRDD.filter(line => {
      val splitVals = line.split(",")
      val from = splitVals(0)
      val to = splitVals(1)

      from.toLong < MAX_FILTER && to.toLong < MAX_FILTER
    })

    val fromRDD = filterRDD.map(line => {
      val splitVals = line.split(",")
      val from = splitVals(0).toLong
      val to = splitVals(1).toLong

      (from, to)
    })

    val toRDD = filterRDD.map(line => {
      val splitVals = line.split(",")

      val to = splitVals(1).toLong
      val from = splitVals(0).toLong

      (to, from)
    })

    val accum = sc.longAccumulator("Triangle Accumulator")

    val path2RDD = joinNodes(toRDD, fromRDD).map(_._2)
    joinNodes(path2RDD, toRDD).map(_._2).foreach { x => if (x._1 == x._2) accum.add(1) }

    print("Triangle: " + accum.value / 3)

  }

  def joinNodes(fromRDD: RDD[(Long, Long)],
                toRDD: RDD[(Long, Long)]): RDD[(Long, (Long, Long))] = {
    val joinedRDD = fromRDD.join(toRDD)
    joinedRDD
  }

}