import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.mllib.classification.{ LogisticRegressionModel }

class LogisticRegressionHelper {
  
  def createLogisticRegressionModel(sc: SparkContext, spamIterable: Iterable[String], 
    hamIterable: Iterable[String], logBool: Boolean): LogisticRegressionModel = {
    
    val dataHandlingHelper = new DataHandlingHelper
    
    if (logBool) {
    	println(">>>>> spam >>>>>")
    	spamIterable.foreach(println)
    	
    	println(">>>>> ham >>>>>")
    	hamIterable.foreach(println)
      
    }

    // Naive bayes model
    val numFeatures = 1000
    val mllibHandlingHelper = new MllibHandlingHelper(numFeatures)

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
    
    mllibHandlingHelper.getLogisticRegressionModel(trainingData)
  }
  
  def testLogisticRegressionModel(mllibHandlingHelper: MllibHandlingHelper, 
      logisticRegressionModel: LogisticRegressionModel, testString: String): Double = {
    val testVector = mllibHandlingHelper.getSingleVector(testString)
    val result = logisticRegressionModel.predict(testVector)
    println(s"Test example: ${testString}")
    println(s"Prediction for test example: ${result}")
    result
  }
  
}