package sparkBasic

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object FiveForcesAnalysis extends App {

    val spark = SparkSession
        .builder
        .appName("myApp")
        .config("spark.mongodb.input.uri", "mongodb://10.120.37.108/project.reactions")
        .config("spark.mongodb.output.uri", "mongodb://10.120.37.108/project.reactions")
        .getOrCreate()


    val df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri","mongodb://10.120.37.108/project.reactions").load()
    val df2 = df.withColumn("politician_id", split(col("post_id"), "_").getItem(0))
    val df3 = df2.groupBy("politician_id", "type").count()
    val df4 = df3.filter($"type" === "HAHA" || $"type" === "SAD" || $"type" === "WOW" || $"type" === "ANGRY" || $"type" === "LOVE")
    val df5 = df4.orderBy($"politician_id", $"count".desc)
    df5.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").option("uri","mongodb://10.120.37.108/statistic_data.five_force").save()

}
