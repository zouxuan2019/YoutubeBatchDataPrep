import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{desc, row_number, sum}
import org.apache.spark.sql.types.IntegerType

object AnalysisProcessor {
  def getTop100VideosWithHighestView(): Unit = {
    val sourcePath = Utils.getDestDir + Utils.getFinalOriginalFileName
    val destPath = Utils.getDestDir + Utils.getTop100FileName
    val sparkSession = Utils.getSparkSession
    val csvDf = sparkSession.read.option("header", "true").csv(sourcePath)
    val df2 = csvDf.withColumn("views", csvDf("views").cast(IntegerType))
    val top100 = df2.sort(desc("views")).limit(100)
    top100.write
      .option("header", "true")
      .option("sep", ",")
      .mode("overwrite")
      .csv(destPath)
    sparkSession.close()
  }

  def getTop100VideosWithHighestViewPerCountry(): Unit = {
    val sourcePath = Utils.getDestDir + Utils.getFinalOriginalFileName
    val destPath = Utils.getDestDir + Utils.getTop100PerCountry
    val sparkSession = Utils.getSparkSession
    val csvDf = sparkSession.read.option("header", "true").csv(sourcePath)
    val df2 = csvDf.withColumn("views", csvDf("views").cast(IntegerType))
    val w = Window.partitionBy("country").orderBy(desc("views"))
    val top100 = df2.withColumn("rn", row_number.over(w))
      .where("rn<=100")
      .drop("rn")
      .sort(desc("country"), desc("views"))
    top100.write
      .option("header", "true")
      .option("sep", ",")
      .mode("overwrite")
      .csv(destPath)
    sparkSession.close()
  }

  def getCategoriesWithViewCount(): Unit = {
    val sourcePath = Utils.getDestDir + Utils.getFinalOriginalFileName
    val destPath = Utils.getDestDir + Utils.getCategoryView
    val sparkSession = Utils.getSparkSession
    val csvDf = sparkSession.read.option("header", "true").csv(sourcePath)
    val df2 = csvDf.withColumn("views", csvDf("views").cast(IntegerType))

    val df3 = df2.groupBy("category").agg(sum("views")).sort(desc("sum(views)")).limit(5)
    val df4 = df3.withColumnRenamed("sum(views)", "sumOfViews")
    df4.write
      .option("header", "true")
      .option("sep", ",")
      .mode("overwrite")
      .csv(destPath)
    sparkSession.close()
  }

  def getVideosWithDislikeGreaterThanLike(): Unit = {
    val sourcePath = Utils.getDestDir + Utils.getFinalOriginalFileName
    val destPath = Utils.getDestDir + Utils.getDislikeOverLike
    val sparkSession = Utils.getSparkSession
    val csvDf = sparkSession.read.option("header", "true").csv(sourcePath)
    val df2 = csvDf.withColumn("dislikes", csvDf("dislikes").cast(IntegerType))
      .withColumn("likes", csvDf("likes").cast(IntegerType))
    val top100 = df2
      .where("dislikes>likes")
      .sort(desc("country"), desc("views"))
    top100.write
      .option("header", "true")
      .option("sep", ",")
      .mode("overwrite")
      .csv(destPath)
    sparkSession.close()
  }

  def getVideosWithMultipleRecords(): Unit = {
    val sourcePath = Utils.getDestDir + Utils.getFinalOriginalFileName
    val destPath = Utils.getDestDir + Utils.getMultipleRecords

    val sparkSession = Utils.getSparkSession
    val csvDf = sparkSession.read.option("header", "true").csv(sourcePath)
    import org.apache.spark.sql.functions._
    val dfTitles = csvDf.groupBy("title").agg(count(lit(1))).withColumnRenamed("count(1)", "cnt").where("cnt>=5")
    val dfResult = csvDf.join(dfTitles, "title")
    dfResult.write
      .option("header", "true")
      .option("sep", ",")
      .mode("overwrite")
      .csv(destPath)
    sparkSession.close()
  }

}
