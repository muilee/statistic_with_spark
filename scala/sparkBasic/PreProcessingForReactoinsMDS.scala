package sparkBasic

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object PreProcessingForReactoinsMDS extends App {
    val spark = SparkSession
        .builder
        .appName("myApp")
        .config("spark.mongodb.input.uri", "mongodb://10.120.37.108/project.reactions")
        .config("spark.mongodb.output.uri", "mongodb://10.120.37.108/project.reactions")
        .getOrCreate()

    import spark.implicits._

    case class Pair(politician_id:String, person_ids:Array[String])

    val df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri","mongodb://10.120.37.108/project.reactions").load()
    val df1 = df.withColumn("politician_id", split(col("post_id"), "_").getItem(0)).cache()
    val table = df1.select($"person_id", $"politician_id")
    val rdd = table.rdd
    val temp1 = rdd.map(x => (x(0).asInstanceOf[String], x(1).asInstanceOf[String])).map(x => (x._1, Set(x._2)) )
    val temp2 = temp1.reduceByKey( (x, y) => x ++ y  )
    val temp3 = temp2.filter(x => x._2.size > 1)
    val temp4 = temp3.map(x => (x._1, x._2.toList.sorted)).flatMapValues(x => x)
    val temp5 = temp4.map(x => (x._2, List(x._1)) ).reduceByKey((x, y) => x ::: y).map(x => (x._1, x._2.sorted.toArray))
    val temp6 = temp5.map(x => Pair(x._1.asInstanceOf[String], x._2))
    val df2 = temp6.toDF()
    val df3 = df2.orderBy("politician_id")
    df3.repartition(1).write.json("file:///home/cloudera/Music/data")

    val person_ids = df1.select("person_id").distinct.orderBy("person_id")

    person_ids.write.format("com.mongodb.spark.sql.DefaultSource").mode("overwrite").option("uri","mongodb://10.120.37.108/temp.reactions_person_ids").save()

    // spark-submit --master spark://quickstart.cloudera:7077 --num-executors 100 --executor-memory 4G \
    // --executor-cores 3 --conf spark.default.parallelism=1000 --conf spark.storage.memoryFraction=0.5 \
    // --conf spark.shuffle.memoryFraction=0.3 --class sparkBasic.PreProcessingForReactoinsMDS \
    // --packages org.mongodb.spark:mongo-spark-connector_2.11:2.2.0 \
    // /home/cloudera/IdeaProjects/spark/out/artifacts/spark/spark.jar\

}
