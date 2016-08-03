import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.SparkContext

class DataHandlingHelper {

  def convertRDDToIterable(m: RDD[String]): Iterable[String] = {
    val map = m.zipWithUniqueId().collect().toMap
    map.map(x => x._1)
  }

  def getSplitDataUsingIterable(i: Iterable[String], s: String, n: Int): Iterable[String] = {
    i.map(x => x.split(s)(n))
  }

  def convertListToRDD(sc: SparkContext, lp: Iterable[LabeledPoint]) = {
    sc.parallelize(lp.toList)
  }

}