import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.classification.{ NaiveBayes, NaiveBayesModel }

class MllibHandlingHelper(numFeaturesVal: Int) {
  val tf = new HashingTF(numFeatures = numFeaturesVal)
  val seperator = " "

  def getFeatures(i: Iterable[String]) = {
    i.map(sentense => tf.transform(sentense.toString().split(seperator)))
  }

  def getTf() = tf

  def getIterableLabelpoint(idNo: Double, i: Iterable[Vector]) = {
    i.map(features => LabeledPoint(idNo, features))
  }

  def getSingleVector(str: String) = {
    tf.transform(str.split(" "))
  }

  def getProcessTrainningData(positiveRDD: RDD[LabeledPoint], negativeRDD: RDD[LabeledPoint]) = {
    val trainingData = positiveRDD ++ negativeRDD
    trainingData.cache()
  }

  def getNaiveBayesModel(trainningRatio: Double, seedVal: Long, trainData: RDD[LabeledPoint]) = {
	  println(">>>>> getNaiveBayesModel >>>>>")
    val splits = trainData.randomSplit(Array(trainningRatio, (1 - trainningRatio)), seed = seedVal)
    val training = splits(0)
    val test = splits(1)
    val model = NaiveBayes.train(training, lambda = 1.0, modelType = "multinomial")
    val predictionAndLabel = test.map(p => (model.predict(p.features), p.label))
    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()
    println(">>>>> getNaiveBayesModel >>>>> accuracy: " + accuracy)
    model
  }
}