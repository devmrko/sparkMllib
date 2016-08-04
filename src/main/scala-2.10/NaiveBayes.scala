import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.lang.Boolean
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.classification.NaiveBayesModel

object NaiveBayesApp {

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

    val dataHandlingHelper = new DataHandlingHelper

    var readJsonFileInfo = "input/spam"
    val jsonRDD = sc.textFile(readJsonFileInfo)
    val spamRDD = jsonRDD.filter(line => line.contains("spam"))
    val hamRDD = jsonRDD.filter(line => line.contains("ham"))
    val spamIterable = dataHandlingHelper.convertRDDToIterable(spamRDD)
    val hamIterable = dataHandlingHelper.convertRDDToIterable(hamRDD)
    val spamSecondColumnIterable = dataHandlingHelper.getSplitDataUsingIterable(spamIterable, "\t", 1);
    val hamSecondColumnIterable = dataHandlingHelper.getSplitDataUsingIterable(hamIterable, "\t", 1);

    val naiveBayesExample = new NaiveBayesExample
    naiveBayesExample.runNaiveBayesModel(sc, spamSecondColumnIterable, hamSecondColumnIterable)

    sc.stop()
  }

}

class NaiveBayesExample {

  def createNaiveBayesModel(sc: SparkContext, mllibHandlingHelper: MllibHandlingHelper, 
      spamIterable: Iterable[String], hamIterable: Iterable[String], trainningRatio: Double, 
      seedVal: Long, logBool: Boolean): NaiveBayesModel = {

    println(">>>>> createNaiveBayesModel >>>>>")

    // Naive bayes model
    val dataHandlingHelper = new DataHandlingHelper

    if (logBool) {
      println(">>>>> spam >>>>>")
      spamIterable.foreach(println)

      println(">>>>> ham >>>>>")
      hamIterable.foreach(println)

    }

    val spamFeatures = mllibHandlingHelper.getFeatures(spamIterable)
    val hamFeatures = mllibHandlingHelper.getFeatures(hamIterable)

    if (logBool) {
      println(">>>>> spamFeatures >>>>>")
      spamFeatures.foreach(println)

      println(">>>>> hamFeatures >>>>>")
      hamFeatures.foreach(println)

    }

    val spamLabeledPoint = mllibHandlingHelper.getIterableLabelpoint(0, spamFeatures)
    val hamLabeledPoint = mllibHandlingHelper.getIterableLabelpoint(1, hamFeatures)

    if (logBool) {
      println(">>>>> positiveExamples >>>>>")
      spamLabeledPoint.foreach(println)

      println(">>>>> negativeExamples >>>>>")
      hamLabeledPoint.foreach(println)

    }

    val spamLabeledPointRDD = dataHandlingHelper.convertListToRDD(sc, spamLabeledPoint)
    val hamLabeledPointRDD = dataHandlingHelper.convertListToRDD(sc, hamLabeledPoint)

    val trainingData = mllibHandlingHelper.getProcessTrainningData(spamLabeledPointRDD, hamLabeledPointRDD)
    mllibHandlingHelper.getNaiveBayesModel(trainningRatio, seedVal, trainingData)

  }

  def testNaiveBayesModel(mllibHandlingHelper: MllibHandlingHelper, 
      naiveBayesTrainedModel: NaiveBayesModel, testString: String): Double = {
    val testVector = mllibHandlingHelper.getSingleVector(testString)
    val result = naiveBayesTrainedModel.predict(testVector)
    println(s"Test example: ${testString}")
    println(s"Prediction for test example: ${result}")
    result
  }

  def runNaiveBayesModel(sc: SparkContext, spamIterable: Iterable[String], hamIterable: Iterable[String]) {

    val numFeatures = 1000
    val mllibHandlingHelper = new MllibHandlingHelper(numFeatures)
    val naiveBayesTrainedModel = createNaiveBayesModel(sc, mllibHandlingHelper, spamIterable, hamIterable, 0.7, 1100L, true)

    val testSpamString01 = "O M G GET cheap stuff by sending money to ..."
    val testSpamString02 = "URGENT! Your Mobile No..."
    val testHamString01 = "Hi Dad, I started studying Spark the other ..."
    val testHamString02 = "Hi Dad, I have cheap stuff by sending money ..."

    testNaiveBayesModel(mllibHandlingHelper, naiveBayesTrainedModel, testSpamString01)
    testNaiveBayesModel(mllibHandlingHelper, naiveBayesTrainedModel, testSpamString02)
    testNaiveBayesModel(mllibHandlingHelper, naiveBayesTrainedModel, testHamString01)
    testNaiveBayesModel(mllibHandlingHelper, naiveBayesTrainedModel, testHamString02)

    //    naiveBayesTrainedModel.save(sc, "target/tmp/myNaiveBayesModel")
    //    val sameModel = NaiveBayesModel.load(sc, "target/tmp/myNaiveBayesModel")
  }
}