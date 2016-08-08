import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object LogisticRegressionApp {

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

    val numFeatures = 1000
    val mllibHandlingHelper = new MllibHandlingHelper(numFeatures)
    val dataHandlingHelper = new DataHandlingHelper

    var readJsonFileInfo = "input/spam"
    val jsonRDD = sc.textFile(readJsonFileInfo)
    val spamRDD = jsonRDD.filter(line => line.contains("spam"))
    val hamRDD = jsonRDD.filter(line => line.contains("ham"))
    val spamIterable = dataHandlingHelper.convertRDDToIterable(spamRDD)
    val hamIterable = dataHandlingHelper.convertRDDToIterable(hamRDD)
    val spamSecondColumnIterable = dataHandlingHelper.getSplitDataUsingIterable(spamIterable, "\t", 1);
    val hamSecondColumnIterable = dataHandlingHelper.getSplitDataUsingIterable(hamIterable, "\t", 1);

    val logisticRegressionHelper = new LogisticRegressionHelper
    val logisticRegressionModel = logisticRegressionHelper.createLogisticRegressionModel(sc, spamSecondColumnIterable, hamSecondColumnIterable, true)

    val testSpamString01 = "O M G GET cheap stuff by sending money to ..."
    val testSpamString02 = "URGENT! Your Mobile No..."
    val testHamString01 = "Hi Dad, I started studying Spark the other ..."
    val testHamString02 = "Hi Dad, I have cheap stuff by sending money ..."

    logisticRegressionHelper.testLogisticRegressionModel(mllibHandlingHelper, logisticRegressionModel, testSpamString01)
    logisticRegressionHelper.testLogisticRegressionModel(mllibHandlingHelper, logisticRegressionModel, testSpamString02)
    logisticRegressionHelper.testLogisticRegressionModel(mllibHandlingHelper, logisticRegressionModel, testHamString01)
    logisticRegressionHelper.testLogisticRegressionModel(mllibHandlingHelper, logisticRegressionModel, testHamString02)

    //    naiveBayesTrainedModel.save(sc, "target/tmp/myNaiveBayesModel")
    //    val sameModel = NaiveBayesModel.load(sc, "target/tmp/myNaiveBayesModel")

    sc.stop()
  }

}

