package com.github.Punctuality

import com.github.Punctuality.model.SugarEnergy
import com.github.Punctuality.util.Decompressing
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkFiles
import org.apache.spark.sql._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.rdd.RDD

import scala.util.Try

object OpenFoodFactsClustering extends LazyLogging {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("TestSparkConnection")
      .master("spark://spark-master:7077")
      .config("spark.sql.caseSensitive", "true")
      .getOrCreate()

    val namenodeAddress = args.head
    spark.sparkContext.addArchive(s"hdfs://$namenodeAddress/openfoodfacts.zip")

    val downloadedArchive = SparkFiles.get("openfoodfacts.zip")

    val rdd = spark.sparkContext.textFile(s"$downloadedArchive/openfoodfacts.jsonl")
    val jsonRDD = rdd.flatMap(str => Try(parse(str)).toOption)
    val sugarEnergyRDD: RDD[SugarEnergy] = jsonRDD.flatMap(json => Try(json.as[SugarEnergy]).toOption).cache()

    sugarEnergyRDD.take(10).foreach(sr => logger.info(sr.toString))

    logger.info(s"Total SE: ${sugarEnergyRDD.count()}")

    import spark.implicits._
    val nutrimentsDS = sugarEnergyRDD.toDS()

    val clusteringData: DataFrame = new VectorAssembler()
      .setInputCols(Array("sugar", "energy"))
      .setOutputCol("features")
      .transform(nutrimentsDS)

    clusteringData.printSchema()

    // Trains a k-means model.
    val kmeans = new KMeans()
      .setK(4)
      .setSeed(1L)
      .setFeaturesCol("features")
      .setPredictionCol("cluster")
    val model = kmeans.fit(clusteringData)

    // Make predictions
    val predictions: DataFrame = model.transform(clusteringData).cache()

    // Evaluate clustering by computing Silhouette score
    val evaluator = new ClusteringEvaluator().setFeaturesCol("features").setPredictionCol("cluster")

    val silhouette = evaluator.evaluate(predictions)
    logger.info(s"Silhouette with squared euclidean distance = $silhouette")

    // Shows the result.
    logger.info("Cluster Centers: ")
    model.clusterCenters.foreach(array => logger.info(array.toString()))

    val groupedDataset = predictions.groupBy("cluster").count().cache()

    groupedDataset.show()
  }
}
