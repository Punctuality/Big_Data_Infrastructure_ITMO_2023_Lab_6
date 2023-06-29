ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.14"

// 2.12.13 > 23/06/05 04:26:45 WARN TaskSetManager: Lost task 1.0 in stage 0.0 (TID 1) (172.18.0.3 executor 0): java.lang.ClassCastException: cannot assign instance of java.lang.invoke.SerializedLambda to field org.apache.spark.rdd.MapPartitionsRDD.f of type scala.Function3 in instance of org.apache.spark.rdd.MapPartitionsRDD
// 2.12.8 java.io.InvalidClassException: scala.collection.mutable.WrappedArray$ofRef; local class incompatible: stream classdesc serialVersionUID = 1028182004549731694, local class serialVersionUID = 3456489343829468865

lazy val root = (project in file("."))
  .settings(
    name := "Lab_5",
    libraryDependencies ++= Seq(
//      Spark dependency
      "org.apache.spark" %% "spark-core" % "3.3.0" % "provided",
      "org.apache.spark" %% "spark-sql" % "3.3.0" % "provided",
      "org.apache.spark" %% "spark-mllib" % "3.3.0" % "provided",

      // Logging
      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",
      "io.circe" %% "circe-core" % "0.14.1",
      "io.circe" %% "circe-jackson" % "0.6.1"
    )
  )