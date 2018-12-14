package edu.neu.ccs

import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Spark Dataset/Dataframe implementation for PageRank
  */
object PageRankDS {

  case class Graph(v1: String, v2: String)

  case class PageRank(v2: String, pageRank: Double)

  /** Main method */
  def main(args: Array[String]): Unit = {
    val startTimeMillis = System.currentTimeMillis()

    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 1) {
      logger.error("Usage:\nedu.neu.ccs.PageRankDS <K>")
      System.exit(1)
    }

    val k = args(0).toInt // no. of vertices

    val spark = SparkSession
      .builder
      .appName("PageRankDS")
      .getOrCreate()

    import spark.implicits._

    val noOfNodes = k * k
    val nodesList = List.range(1, noOfNodes + 2)
    val initialPR = 1.0 / noOfNodes

    val edges = toPairs(nodesList, k)
    val ranks = List.range(0, noOfNodes + 1).map(x => {
      if (x == 0)
        PageRank(x.toString, 0.0)
      else
        PageRank(x.toString, initialPR)
    })

    val graphDS = edges.toDS().cache()
    var rankDS = ranks.toDS()

    val iters = 10

    for (i <- 1 to iters) {

      val path2DS: DataFrame =
        graphDS.as("g").join(rankDS.as("r"),
          $"g.v1" === $"r.v2", "left_outer")

      val df2: Dataset[(String, Double)] = path2DS.flatMap(r =>
        if (r.getString(0).toInt % k == 1)
          List((r.getString(0), 0.0), (r.getString(1), r.getDouble(3)))
        else
          List((r.getString(1), r.getDouble(3)))
      )

      val df1 = df2.groupBy("_1").sum()
      val delta = df1.where($"_1" === "0").select($"sum(_2)").first().getDouble(0)

      rankDS = df1.map(r => {
        if (r.getString(0).equals("0")) {
          PageRank(r.getString(0), r.getDouble(1))
        } else {
          PageRank(r.getString(0), r.getDouble(1) + delta * initialPR)
        }
      })

      val pr = rankDS.where($"v2" =!= "0").select($"pageRank").as[Double].collect().sum
      println(pr)

    }

    logger.warn("*****************************************************************************************************")

    val topKRDDByPR = rankDS.rdd.filter(_.v2 != "0").takeOrdered(k)(Ordering[Double].reverse.on { x => x.pageRank })
    logger.warn("Top K Page Rank values:")
    logger.warn(topKRDDByPR.foreach(println))

    logger.warn("*****************************************************************************************************")

    val topKRDD = rankDS.rdd.takeOrdered(k + 1)(Ordering[Int].on { x => x.v2.toInt })
    logger.warn("Top K values:")
    logger.warn(topKRDD.foreach(println))

    logger.warn("*****************************************************************************************************")
    logger.warn("Execution Time:" + (System.currentTimeMillis() - startTimeMillis) + "ms.")

    spark.stop()
  }

  def toPairs(a: Seq[Int], k: Int): List[Graph] = {
    a.sliding(2).map(x => {
      if (x.head % k == 0) {
        Graph(x.head.toString, "0")
      } else {
        Graph(x.head.toString, x.tail.head.toString)
      }
    }).toList
  }

}