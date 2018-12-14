package edu.neu.ccs

import org.apache.log4j.LogManager
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
  * Spark implementation for Twitter followers count program with different ways of combining
  */
object TwitterFollowersCount {

  // schema for twitter follower dataset
  case class TwitterFollowers(follower_id: Long, followee_id: Long)

  val MAX_FILTER = 20000

  /** Main method */
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder.master("yarn")
      .appName("Twitter Followers Count")
      .config("spark.logLineage", "true")
      .getOrCreate()

    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nwc.TwitterFollowersCountMain <input dir> <output dir> <combine type>")
      System.exit(1)
    }

    groupByDataset(args(0), args(1), spark)
  }

  def groupByDataset(input: String, output: String, spark: SparkSession) { // << add this
    import spark.implicits._

    val customSchema = StructType(Array(
      StructField("follower_id", LongType, nullable = false),
      StructField("followee_id", LongType, nullable = false)))

    val followersDS = spark.read.format("csv").schema(customSchema).
      load(input)
      .where($"follower_id" < MAX_FILTER && $"followee_id" < MAX_FILTER)

    val path2DS: Dataset[(Row, Row)] =
      followersDS.as("a").joinWith(followersDS.as("b"),
        $"a.followee_id" === $"b.follower_id")

    val triangleDS: Dataset[((Row, Row), Row)] =
      path2DS.as("a").joinWith(followersDS.as("b"),
        $"a._1.follower_id" === $"b.followee_id" && $"a._2.followee_id" === $"b.follower_id")

    print("Triangles: " + triangleDS.count() / 3)
  }

}