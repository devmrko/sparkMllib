import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SparkApp {

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

    val naiveBayesExample = new NaiveBayesExample
    naiveBayesExample.runNaiveBayesModel(sc, jsonRDD)

    sc.stop()
  }

}