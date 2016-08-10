import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.sql.{ Connection, DriverManager, ResultSet };
import scala.collection.mutable.ListBuffer

object SparkMllibLogsticRegApp {

  val usage = """
    Usage: mmlaln [--min-size num] [--max-size num] filename
  """

  def main(args: Array[String]) {

    if (args.length == 0) println(usage)
    val arglist = args.toList
    type OptionMap = Map[Symbol, Any]

    def nextOption(map: OptionMap, list: List[String]): OptionMap = {
      def isSwitch(s: String) = (s(0) == '-')
      list match {
        case Nil => map
        case "--max-size" :: value :: tail =>
          nextOption(map ++ Map('maxsize -> value.toInt), tail)
        case "--min-size" :: value :: tail =>
          nextOption(map ++ Map('minsize -> value.toInt), tail)
        case string :: opt2 :: tail if isSwitch(opt2) =>
          nextOption(map ++ Map('infile -> string), list.tail)
        case string :: Nil => nextOption(map ++ Map('infile -> string), list.tail)
        case option :: tail =>
          println("Unknown option " + option)
          exit(1)
      }
    }
    val options = nextOption(Map(), arglist)
    println(options)

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

    var distinctQuery = "SELECT	url FROM blogContentsInfo WHERE job_id = "

    var getSingleUrlContentsQuery = "SELECT	name FROM result "
    getSingleUrlContentsQuery += "WHERE type not in ('SF', 'SN', 'JKB', 'EC', 'ETM', 'JKS', 'JX', 'JKO', 'JKG', 'SSO') "
    getSingleUrlContentsQuery += "AND url = "

    var getSingleUrlContentsQueryForSpam = "SELECT name FROM spam "
    getSingleUrlContentsQueryForSpam += "WHERE type not in ('SF', 'SN', 'JKB', 'EC', 'ETM', 'JKS', 'JX', 'JKO', 'JKG', 'SSO') "
    getSingleUrlContentsQueryForSpam += "AND url = "

    val mySqlHelper = new MySqlHelper
    // for update: ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE
    val mySqlConn = mySqlHelper.getMySqlConn(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

    val normalSentenseListBuffer: ListBuffer[String] = ListBuffer()
    val spamSentenseListBuffer: ListBuffer[String] = ListBuffer()

    try {
      val prep = mySqlConn.prepareStatement(distinctQuery + "'1'")
      val resultSet = prep.executeQuery()

      val df = sqlContext.read.parquet("seunjeonResult.parquet")
      df.registerTempTable("result")
      while (resultSet.next) {
        val baseDf = sqlContext.sql(getSingleUrlContentsQuery + "'" + resultSet.getString("url") + "'").toDF()
        val tempStr = baseDf.map(elem => elem.get(0) + " ").reduce(_ + _)
        println(tempStr)
        if (!tempStr.isEmpty()) {
          normalSentenseListBuffer += tempStr

        }

      }

      val spamPrep = mySqlConn.prepareStatement("SELECT	url FROM blogContentsInfo WHERE job_id IN ('2', '3')")
      val spamResultSet = spamPrep.executeQuery()

      val spamDf = sqlContext.read.parquet("spamResult.parquet")
      spamDf.registerTempTable("spam")
      while (spamResultSet.next) {
        val baseDf = sqlContext.sql(getSingleUrlContentsQueryForSpam + "'" + spamResultSet.getString("url") + "'").toDF()
        val tempStr = baseDf.map(elem => elem.get(0) + " ").reduce(_ + _)
        println(tempStr)
        if (!tempStr.isEmpty()) {
          spamSentenseListBuffer += tempStr

        }

      }

    } finally {
      mySqlConn.close

    }

    val normalList = normalSentenseListBuffer.toList
    val spamList = spamSentenseListBuffer.toList

    println(">>>>> normalSentense >>>>>")
    normalList.foreach(println)
    println(">>>>> spamSentense >>>>>")
    spamList.foreach(println)

    runNaiveBayesModelForPjt(sc, normalList, spamList)

    sc.stop()
  }

  def runNaiveBayesModelForPjt(sc: SparkContext, spamIterable: Iterable[String], hamIterable: Iterable[String]) {

    val numFeatures = 1000
    val mllibHandlingHelper = new MllibHandlingHelper(numFeatures)
    val logisticRegressionHelper = new LogisticRegressionHelper

    val logisticRegressionModel = logisticRegressionHelper.createLogisticRegressionModel(sc, spamIterable, hamIterable, true)

    logisticRegressionModel.save(sc, "target/tmp/myNaiveBayesModel04")
    //    val sameModel = NaiveBayesModel.load(sc, "target/tmp/myNaiveBayesModel01")
  }

}