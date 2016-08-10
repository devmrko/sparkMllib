import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.sql.{ Connection, DriverManager, ResultSet };
import org.apache.spark.mllib.classification.{ NaiveBayes, NaiveBayesModel, LogisticRegressionModel }
import scala.collection.mutable.ListBuffer

object NaiveModelTestApp {

  def main(args: Array[String]) {

    val conf = new SparkConf()

    if (args.isEmpty) {
      conf.setAppName("SparkApp").setMaster("local")
      println(">>>>> IDE development mode >>>>>")

    } else if (args(0).equals("cluster")) {
      println(">>>>> cluster mode >>>>>")

    }

    val modelPath = "target/tmp/myNaiveBayesModel02"
    val job_id = 1

    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val dataHandlingHelper = new DataHandlingHelper
    val naiveBayesHelper = new NaiveBayesHelper
    val numFeatures = 1000
    val mllibHandlingHelper = new MllibHandlingHelper(numFeatures)

    val testModel = NaiveBayesModel.load(sc, modelPath)

    var distinctQuery = "SELECT	url FROM blogContentsInfo WHERE job_id = "

    var getSingleUrlContentsQuery = "SELECT	c.name FROM ( "
    getSingleUrlContentsQuery += "SELECT	a.url, a.name, a.type FROM result a "
    getSingleUrlContentsQuery += "UNION "
    getSingleUrlContentsQuery += "SELECT	b.url, b.name, b.type FROM spam b "
    getSingleUrlContentsQuery += ") c "
    getSingleUrlContentsQuery += "WHERE c.type not in ('SF', 'SN', 'JKB', 'EC', 'ETM', 'JKS', 'JX', 'JKO', 'JKG', 'SSO') "
    getSingleUrlContentsQuery += "AND c.url = "
    
    val mySqlHelper = new MySqlHelper
    val mySqlConn = mySqlHelper.getMySqlConn(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)

    try {
      val prep = mySqlConn.prepareStatement(distinctQuery + "'" + job_id + "'")
      val resultSet = prep.executeQuery()

      val df = sqlContext.read.parquet("seunjeonResult.parquet")
      df.registerTempTable("result")
      val spamDf = sqlContext.read.parquet("spamResult.parquet")
      spamDf.registerTempTable("spam")

      while (resultSet.next) {
        val url = resultSet.getString("url")
        val baseDf = sqlContext.sql(getSingleUrlContentsQuery + "'" + url + "'").toDF()
        val tempStr = baseDf.map(elem => elem.get(0) + " ").reduce(_ + _)
        
        val result = naiveBayesHelper.testNaiveBayesModel(mllibHandlingHelper, testModel, tempStr)
        
        val queryStmt = "INSERT INTO modelTestResult (url, model_id, result, reg_id, reg_date) VALUES (?, ?, ?, ?, now()) "
        val prep = mySqlConn.prepareStatement(queryStmt)
        prep.setString(1, url)
        prep.setString(2, modelPath)
        prep.setString(3, result.toString())
        prep.setString(4, "system")
        prep.executeUpdate

      }

    } finally {
      mySqlConn.close

    }

    sc.stop()
  }

}