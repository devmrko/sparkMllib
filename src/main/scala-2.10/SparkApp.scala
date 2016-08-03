import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SparkApp {

  def main(args: Array[String]) {

    val conf = new SparkConf()
    conf.setAppName("SparkApp").setMaster("local")

    var mlModelKind = "naive"

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

    if (mlModelKind.equals("naive")) {
      val naiveBayesExample = new NaiveBayesExample
      naiveBayesExample.runNaiveBayesModel(sc, jsonRDD)
      
    } else if (mlModelKind.equals("logisticRegression")) {
      val logisticRegressionExample = new LogisticRegressionExample
      logisticRegressionExample.runLogisticRegression(sc, jsonRDD)
      
    } else if (mlModelKind.equals("TFIDF")) {
      val tFIDFExample = new TFIDFExample
      tFIDFExample.runTFIDF(sc, jsonRDD)
      
    }

    sc.stop()
  }

}