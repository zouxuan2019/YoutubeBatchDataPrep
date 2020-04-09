import org.apache.spark.sql.functions.{col, explode, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}


object DataPreProcessor {

  def preProcessOriginalDataset() = {
    val countryList = Array("CA", "DE", "FR", "GB", "IN", "JP", "KR", "MX", "RU", "US")
    val sourceDataDir = Utils.getSourceDir
    val destPath = Utils.getDestDir + Utils.getFinalOriginalFileName
    val sparkSession = Utils.getSparkSession
    val finalResult = countryList.map(country =>
      getDatasetWithCategoryDescDf(country, sourceDataDir, sparkSession)
    )
      .reduce(_.union(_))
    finalResult
      .coalesce(1)
      .write
      .option("header", "true")
      .option("sep", ",")
      .mode("overwrite")
      .csv(destPath)
    sparkSession.close()
  }


  private def getDatasetWithCategoryDescDf(country: String, dataDir: String, sparkSession: SparkSession) = {
    val jsonDf: DataFrame = sparkSession.read.option("multiline", "true").json(dataDir + country + "_category_id.json")
    val categoryDf = jsonDf.select(explode(jsonDf("items"))).select("col.id", "col.snippet.title").withColumnRenamed("title", "category")
    val csvDf = sparkSession.read.option("header", "true").csv(dataDir + country + "videos.csv")
    val joinedDf = categoryDf.join(csvDf, col("id") === col("category_id"), "inner").withColumn("country", lit(country))
    import org.apache.spark.sql.functions._
    val finalDf = joinedDf
      .withColumn("tags", regexp_replace(col("tags"), "\\[none\\]", ""))
      .withColumn("tags", regexp_replace(col("tags"), "[^a-z A-Z|]", ""))
      .withColumn("tags", regexp_replace(col("tags"), "\\|{2}", "|"))
      .select("country", "trending_date", "title",
        "channel_title", "category",
        "publish_time", "tags",
        "views", "likes",
        "dislikes", "comment_count"
      )
    import org.apache.spark.sql.functions._
    val distinctFinalDf = finalDf
      .groupBy("country", "title", "channel_title", "category", "tags")
      .agg(max("publish_time").alias("publish_time")
        , max("views").alias("views")
        , max("likes").alias("likes")
        , max("dislikes").alias("dislikes")
        , max("comment_count").alias("comment_count"))
    distinctFinalDf
  }
}
