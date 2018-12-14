package edu.neu.ccs

import org.apache.log4j.LogManager
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, StructField, StructType}

/**
  * Spark implementation for Twitter followers count program with different ways of combining
  */
object TwitterFollowersCount {

  // schema for twitter follower dataset
  case class TwitterFollowers(follower_id: Long, followee_id: Long)

  /** Main method */
  def main(args: Array[String]) {
    val startTimeMillis = System.currentTimeMillis()

    val spark = SparkSession
      .builder()
      .appName("Twitter Followers Count")
      .config("spark.logLineage", "true")
      .getOrCreate()

    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 3) {
      logger.error("Usage:\nwc.TwitterFollowersCountMain <input dir> <output dir> <combine type>")
      System.exit(1)
    }

    val sc = spark.sparkContext

    args(2) match {
      case "RDD-G" => rddGroupByKey(args(0), args(1), sc)
      case "RDD-R" => rddReduceByKey(args(0), args(1), sc)
      case "RDD-F" => rddFoldByKey(args(0), args(1), sc)
      case "RDD-A" => rddAggByKey(args(0), args(1), sc)
      case "DSET" => groupByDataset(args(0), args(1), spark)
      case whatisthis =>
        rddGroupByKey(args(0), args(1), sc)
        print("Unexpected Input:", whatisthis)
    }

    print("Duration:" + (System.currentTimeMillis() - startTimeMillis) + "ms.")
  }

  def groupByDataset(input: String, output: String, spark: SparkSession) {
    val customSchema = StructType(Array(
      StructField("follower_id", LongType, nullable = false),
      StructField("followee_id", LongType, nullable = false)))

    val followersDS = spark.read.format("csv").schema(customSchema).load(input)

    val groupedDS = followersDS.groupBy("followee_id").count()
    groupedDS.explain(extended = true)
    groupedDS.write.csv(output)
  }

  def rddAggByKey(input: String, output: String, sc: SparkContext) {
    val listRDD = sc.textFile(input)

    val pairsRDD = listRDD.map(line => (line.split(",")(1), 1)) // split on ',' and pick 2nd token as a key
    val aggRDD = pairsRDD.aggregateByKey(0)(_ + _, _ + _) // reduce on the key

    aggRDD.saveAsTextFile(output)
  }

  def rddFoldByKey(input: String, output: String, sc: SparkContext) {
    val listRDD = sc.textFile(input)
    val pairsRDD = listRDD.map(line => (line.split(",")(1), 1)) // split on ',' and pick 2nd token as a key
    val foldRDD = pairsRDD.foldByKey(0)(_ + _) // reduce on the key

    foldRDD.saveAsTextFile(output)
  }

  def rddGroupByKey(input: String, output: String, sc: SparkContext) {
    val listRDD = sc.textFile(input)
    val pairsRDD = listRDD.map(line => (line.split(",")(1), 1)) // split on ',' and pick 2nd token as a key
    val groupedRDD = pairsRDD.groupByKey() // reduce on the key

    val mappedRDD = groupedRDD.map(t => (t._1, t._2.sum))

    mappedRDD.saveAsTextFile(output)
  }

  def rddReduceByKey(input: String, output: String, sc: SparkContext) {
    val listRDD = sc.textFile(input)

    val pairsRDD = listRDD.map(line => (line, 1)) // split on ',' and pick 2nd token as a key
    val reducedRDD = pairsRDD.reduceByKey(_ + _) // reduce on the key

    reducedRDD.coalesce(1).saveAsTextFile(output)
  }

}