package sparkBasic

import org.apache.spark.sql.SparkSession

object ReactionsMDS extends App {

    val spark = SparkSession
        .builder
        .appName("myApp")
        .config("spark.mongodb.input.uri", "mongodb://10.120.37.108/project.reactions")
        .config("spark.mongodb.output.uri", "mongodb://10.120.37.108/project.reactions")
        .getOrCreate()

    import spark.implicits._

    case class Pair(pair: Array[String], number: Int)

    val df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri","mongodb://10.120.37.108/project.reactions").load()
    val table = df.select($"person_id", $"post_id")
    val rdd = table.rdd
    val temp1 = rdd.map(x => (x(0).asInstanceOf[String], x(1).asInstanceOf[String].split("_")(0))).map(x => (x._1, Set(x._2)) )
    val temp2 = temp1.reduceByKey( (x, y) => x ++ y  )
    val temp3 = temp2.filter(x => x._2.size > 1)
    val temp4 = temp3.map(x => x._2.toList.sorted)
    val temp5 = temp4.flatMap(x => x.combinations(2).toList)
    val temp6 = temp5.map(x => (x, 1))
    val temp7 = temp6.reduceByKey((x, y) => x + y).cache()
    val temp8 = temp7.sortBy(x => x._2, false)
    val temp9 = temp8.map(x => Pair(Array(x._1._1.asInstanceOf[String], x._1._2.asInstanceOf[String]), 1))

    val df2 = temp9.toDF()
    df2.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").option("uri","mongodb://10.120.37.108/temp.reactions_pair").save()

}
