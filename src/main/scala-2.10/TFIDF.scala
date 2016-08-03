import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.ml.feature.{ IDF, Tokenizer }
import org.apache.spark.ml.feature.HashingTF

class TFIDFExample {

  def runTFIDF(sc: SparkContext, jsonRDD: RDD[String]) {

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val sentenceData = sqlContext.createDataFrame(Seq(
      (0, "Hi I heard about Spark"),
      (0, "I wish Java could use case classes"),
      (1, "Logistic regression models are neat"))).toDF("label", "sentence")

    // declare tokenizer
    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    // put a dataframe made by mock of query result to tokenize sentenc 
    val wordsData = tokenizer.transform(sentenceData)

    println(">>>>> tokenizer result >>>>>")
    wordsData.foreach(println)

    /**
     * [Term frequency-inverse document frequency (TF-IDF)]
     * : 단어 빈도와 역빈도를 이용하여 특정 단어의 대표성을 뽑아낼 때 사용 (예: 검색엔진 검색어)
     * TF: 하나의 문서에 단어가 등장하는 빈도, HashingTF로 단어별 빈도를 계산
     * IDF: 전체의 문서군에서 얼마나 자주 단어가 등장하는지에 대한 빈도, IDF model에서 feature, vector 컬럼을 받는다
     */

    // assign feature value in proportion to its size of words
    val featureValue = 20
    // declare hashingTF
    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(featureValue)
    // get the result of hashingTF of wordsData
    val featurizedData = hashingTF.transform(wordsData)

    println(">>>>> hashingTF result >>>>>")
    featurizedData.foreach(println)

    // declare idf
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    // get the result of idf of hashingtf's result
    val idfModel = idf.fit(featurizedData)
    val rescaledData = idfModel.transform(featurizedData)

    println(">>>>> idf result >>>>>")
    rescaledData.foreach(println)

    println(">>>>> original result of idf result >>>>>")
    rescaledData.select("features", "label").take(3).foreach(println)
  }

}