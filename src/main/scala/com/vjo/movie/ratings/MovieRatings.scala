package com.vjo.movie.ratings

import java.io.FileInputStream
import java.util.Properties

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}


object MovieRatings {
  def main(args: Array[String]): Unit = {
    //read properties and process movie ratings
    val props = new Properties()
    props.load(new FileInputStream(args(0)))
    processMovieRatings(props)
  }

  def processMovieRatings(prop: Properties): Unit = {
    val separator = prop.getOrDefault("separator", "::").asInstanceOf[String]
    val moviesFilePath = prop.getProperty("moviesFilePath")
    val ratingsFilePath = prop.getProperty("ratingsFilePath")
    val moviesOutputPath = prop.getProperty("moviesOutputPath")
    val ratingsOutputPath = prop.getProperty("ratingsOutputPath")
    val movieRatingsOutputPath = prop.getProperty("movieRatingsOutputPath")
    val top3MoviesByUserOutputPath = prop.getProperty("top3MoviesByUserOutputPath")
    val noOfOutputPartitions = prop.getProperty("noOfOutputPartitions").toInt
    //create spark session
    val spark = SparkSession.builder().appName("MovieRatings").getOrCreate()
    //setting shuffle partitions for aggregations
    spark.conf.set("spark.sql.shuffle.partitions", spark.sparkContext.getConf.getInt("spark.executor.instances", 2))
    import spark.implicits._

    // read input files and write as parquet
    val movieSchema = new StructType().add("movieId", LongType).add("title", StringType).add("Genres", StringType)
    val movies = writeFileBySchema(spark, moviesFilePath, moviesOutputPath, separator, movieSchema, noOfOutputPartitions)
    val ratingSchema = new StructType().add("userId", LongType).add("movieId", LongType).add("rating", IntegerType).add("timestamp", LongType)
    val ratings = writeFileBySchema(spark, ratingsFilePath, ratingsOutputPath, separator, ratingSchema, noOfOutputPartitions)

    //summarize ratings by movie
    val ratingsSummaryByMovie = ratings.groupBy($"movieId").agg(max($"rating").alias("max_ratings"), min($"rating").alias("min_ratings"), avg($"rating").alias("avg_ratings")).cache()
    //join with movies DF and write it
    val movieRatings = movies.join(ratingsSummaryByMovie.hint("broadcast"), "movieId")
    writeAsParquet(movieRatings, movieRatingsOutputPath, noOfOutputPartitions)


    //top 3 movies of each user listed in the ratings movie
    val windowSpec = Window.partitionBy("userId").orderBy($"rating".desc)
    val ranks = ratings.withColumn("rank", rank().over(windowSpec)).withColumn("row_number", row_number().over(windowSpec)).filter($"rank" <= 3 and $"row_number" <= 3).drop("rank", "row_number")
    writeAsParquet(ranks, top3MoviesByUserOutputPath, noOfOutputPartitions)
  }

  def writeFileBySchema(session: SparkSession, inputPath: String, outputPath: String, separator: String, schema: StructType, noOfOutputPartitions: Int): DataFrame = {
    //read input files and write as parquet
    val inputDF = session.read.option("header", "false").option("delimiter", separator).schema(schema).csv(inputPath).cache()
    writeAsParquet(inputDF, outputPath, noOfOutputPartitions)
  }

  def writeAsParquet(dataFrame: DataFrame, outputPath: String, noOfOutputPartitions: Int): DataFrame = {
    //update partitions before writing the file if required
    val inputDF = if (noOfOutputPartitions != -1) dataFrame.coalesce(noOfOutputPartitions) else dataFrame
    inputDF.write.parquet(outputPath)
    inputDF
  }


}
