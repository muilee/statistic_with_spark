package sparkBasic

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ReactionPerPolitician {
    def main(args: Array[String]): Unit = {

        val spark = SparkSession
            .builder
            .appName("myApp")
            .config("spark.mongodb.input.uri", "mongodb://10.120.37.108/project.reactions")
            .config("spark.mongodb.output.uri", "mongodb://10.120.37.108/project.reactions")
            .getOrCreate()


        val df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri","mongodb://10.120.37.108/project.reactions").load()
        val df2 = df.withColumn("politician_id", split(col("post_id"), "_").getItem(0))
        val df3 = df2.groupBy("politician_id", "person_id", "name").count()
        df3.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").option("database","fb_cleaned").option("collection", "test").save()





    }
}
