name := "YoutubeBatchDataPrep"

version := "0.1"

scalaVersion := "2.12.7"

val sparkVersion = "2.4.5"


resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "com.itextpdf" % "itextpdf" % "5.5.6",
  "org.jfree" % "jfreesvg" % "3.0",
  "com.databricks" % "spark-csv_2.11" % "1.4.0",
  "com.github.wookietreiber" % "scala-chart_2.11" % "0.5.0"
)
