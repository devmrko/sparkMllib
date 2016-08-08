import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.lang.Boolean
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.classification.NaiveBayesModel

class NaiveBayesHelper {

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
  
}