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
    val MAX_FILTER = 20000
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
      val from = splitVals(0)
      val to = splitVals(1)

      (from, to)
    })

    val toRDD = filterRDD.map(line => {
      val splitVals = line.split(",")

      val to = splitVals(1)
      val from = splitVals(0)

      (from, to)
    })

    val accum = sc.longAccumulator("Triangle Accumulator")

    val edgesRDD = fromRDD.collect().groupBy { case (from, to) => to }
    // broadcast one of the RDD
    val broadCastVal = sc.broadcast(edgesRDD)

    val path2edges = toRDD.mapPartitions(iter => {
      iter map { case (fromNode, toNode) => {
        if (broadCastVal.value.get(fromNode).isDefined) {
          // look up against set of nodes
          val z: Array[(String, String)] = broadCastVal.value(fromNode)
          z.foreach { case (mapFrom, mapTo) => {
            if (broadCastVal.value.get(mapFrom).isDefined) {
              val x: Array[(String, String)] = broadCastVal.value(mapFrom)
              x.foreach { case (f, t) => {
                if (toNode.equals(f)) {
                  accum.add(1)
                }
              }
              }
            }
          }
          }
        }
      }
      }
    }, preservesPartitioning = true)

    path2edges.collect().map(println)
    println("Triangles:", accum.value / 3)

  }

}