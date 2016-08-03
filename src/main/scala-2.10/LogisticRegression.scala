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

    var readJsonFileInfo = "input/spam"
    val jsonRDD = sc.textFile(readJsonFileInfo)

    val logisticRegressionExample = new LogisticRegressionExample
    logisticRegressionExample.runLogisticRegression(sc, jsonRDD)

    sc.stop()
  }

}

class LogisticRegressionExample {
  
  def runLogisticRegression(sc: SparkContext, jsonRDD: RDD[String]) {
    
    val spamRDD = jsonRDD.filter(line => line.contains("spam"))
    val hamRDD = jsonRDD.filter(line => line.contains("ham"))

    val dataHandlingHelper = new DataHandlingHelper
    val spamIterable = dataHandlingHelper.convertRDDToIterable(spamRDD)
    val hamIterable = dataHandlingHelper.convertRDDToIterable(hamRDD)

    val spamSecondColumnIterable = dataHandlingHelper.getSplitDataUsingIterable(spamIterable, "\t", 1);
    val hamSecondColumnIterable = dataHandlingHelper.getSplitDataUsingIterable(hamIterable, "\t", 1);

    println(">>>>> spam >>>>>")
    spamSecondColumnIterable.foreach(println)

    println(">>>>> ham >>>>>")
    hamSecondColumnIterable.foreach(println)

    // Naive bayes model
    val numFeatures = 1000
    val mllibHandlingHelper = new MllibHandlingHelper(numFeatures)

    val spamFeatures = mllibHandlingHelper.getFeatures(spamSecondColumnIterable)
    val hamFeatures = mllibHandlingHelper.getFeatures(hamSecondColumnIterable)

    println(">>>>> spamFeatures >>>>>")
    spamFeatures.foreach(println)

    println(">>>>> hamFeatures >>>>>")
    hamFeatures.foreach(println)

    val spamLabeledPoint = mllibHandlingHelper.getIterableLabelpoint(0, spamFeatures)
    val hamLabeledPoint = mllibHandlingHelper.getIterableLabelpoint(1, hamFeatures)

    println(">>>>> positiveExamples >>>>>")
    spamLabeledPoint.foreach(println)

    println(">>>>> negativeExamples >>>>>")
    hamLabeledPoint.foreach(println)

    val spamLabeledPointRDD = dataHandlingHelper.convertListToRDD(sc, spamLabeledPoint)
    val hamLabeledPointRDD = dataHandlingHelper.convertListToRDD(sc, hamLabeledPoint)

    val trainingData = mllibHandlingHelper.getProcessTrainningData(spamLabeledPointRDD, hamLabeledPointRDD)
    
    val logisticRegressionWithSGDModel = mllibHandlingHelper.getLogisticRegressionModel(trainingData)
    
    val spamTestExampleVector01 = mllibHandlingHelper.getSingleVector("O M G GET cheap stuff by sending money to ...")
    val spamTestExampleVector02 = mllibHandlingHelper.getSingleVector("URGENT! Your Mobile No...")
    val hamTestExampleVector01 = mllibHandlingHelper.getSingleVector("Hi Dad, I started studying Spark the other ...")
    val hamTestExampleVector02 = mllibHandlingHelper.getSingleVector("Hi Dad, I have cheap stuff by sending money ...")

    println(s"Prediction for positive test example: ${logisticRegressionWithSGDModel.predict(spamTestExampleVector01)}")
    println(s"Prediction for negative test example: ${logisticRegressionWithSGDModel.predict(hamTestExampleVector01)}")
    println(s"Prediction for another negative test example: ${logisticRegressionWithSGDModel.predict(spamTestExampleVector02)}")
    println(s"Prediction for another positive test example: ${logisticRegressionWithSGDModel.predict(hamTestExampleVector02)}")

    //    naiveBayesTrainedModel.save(sc, "target/tmp/myNaiveBayesModel")
    //    val sameModel = NaiveBayesModel.load(sc, "target/tmp/myNaiveBayesModel")
  }
}