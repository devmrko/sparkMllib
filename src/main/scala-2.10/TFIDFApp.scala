import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.ml.feature.{ IDF, Tokenizer }
import org.apache.spark.ml.feature.HashingTF

object TFIDFApp {

  def main(args: Array[String]) {

    val conf = new SparkConf()

    if (args.isEmpty) {
      conf.setAppName("SparkApp").setMaster("local")
      println(">>>>> IDE development mode >>>>>")

    } else if (args(0).equals("cluster")) {
      println(">>>>> cluster mode >>>>>")

    }

    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    var readJsonFileInfo = "input/spam"
    val jsonRDD = sc.textFile(readJsonFileInfo)

    val sentenceData = Seq(
      (0, "Hi I heard about Spark"),
      (0, "I wish Java could use case classes"),
      (1, "Logistic regression models are neat"))

    val tFIDFHelper = new TFIDFHelper
    tFIDFHelper.runTFIDF(sc, sentenceData)

    sc.stop()
  }

}