package sparkBasic

import org.apache.spark._

object AvgPrice {

    def isGood(record: String) = {
        try{
            val temp = record.split(",")(7).toDouble
            true
        }catch {
            case _: Exception => false
        }
    }

    def main(args: Array[String]): Unit = {
        val sc = new SparkContext()
        val records = sc.textFile("file:///home/cloudera/IdeaProjects/spark/res/")
        val header = records.first()
        val recordsWithoutHeader = records.filter( line => line != header)

        val goodRecords = recordsWithoutHeader.filter(isGood)
        val prices = goodRecords.map(record => (record.split(",")(2), record.split(",")(7).toDouble))
        val result = prices.combineByKey((value: Double) => (value, 1),
                                         (acc: (Double, Int), value) => (acc._1 + value, acc._2 + 1),
                                         (acc1:(Double, Int), acc2: (Double, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)).
                                          map(x => (x._1, x._2._1 / x._2._2))
        result.saveAsTextFile("file:///home/cloudera/IdeaProjects/spark/data")
    }
}
