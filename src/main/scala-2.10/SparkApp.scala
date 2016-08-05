import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.sql.{ Connection, DriverManager, ResultSet };

object SparkApp {

  def main(args: Array[String]) {

    val conf = new SparkConf()

    if (args.isEmpty) {
      conf.setAppName("SparkApp").setMaster("local")
      println(">>>>> IDE development mode >>>>>")

    } else if (args(0).equals("cluster")) {
      println(">>>>> cluster mode >>>>>")

    }
    val sc = new SparkContext(conf)
    //    val sqlContext = new HiveContext(sc)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val dataHandlingHelper = new DataHandlingHelper

    var query = "SELECT	name, type, count(*) "
    query += "FROM result "
    query += "where type not in ('SF', 'SN', 'JKB', 'EC', 'ETM', 'JKS', 'JX', 'JKO', 'JKG', 'SSO') "
    //    query += "and type = 'NNG' "
    query += "group by name, type "
    query += "order by count(*) desc "
    query += "limit 500 "

    var distinctQuery = "SELECT	url FROM blogContentsInfo WHERE job_id = "
    var getSingleUrlContentsQuery = "SELECT	name FROM result "
    getSingleUrlContentsQuery += "WHERE type not in ('SF', 'SN', 'JKB', 'EC', 'ETM', 'JKS', 'JX', 'JKO', 'JKG', 'SSO') "
    getSingleUrlContentsQuery += "AND url = "

    val mysqlUrl = "jdbc:mysql://192.168.10.33/master";
    val userName = "hadoop"
    val passwd = "hadoop"

    val prop = new java.util.Properties
    prop.setProperty("user", userName)
    prop.setProperty("password", passwd)

    //    val blogContentsInfoTable = sqlContext.read.jdbc(mysqlUrl, "blogContentsInfo", prop)
    //    val stopwordsTable = sqlContext.read.jdbc(mysqlUrl, "stopwords", Array("del_yn='N'"), prop)
    //    stopwordsTable.foreach(println)

    val mySqlConn = DriverManager.getConnection(mysqlUrl + "?user=" + userName + "&password=" + passwd)
    val statement = mySqlConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
    //    val statement = mySqlConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)

    val normalSentense: Iterable[String] = Nil
    val spamSentense: Iterable[String] = Nil
    
    try {
      val prep = mySqlConn.prepareStatement(distinctQuery + "'1'")
      val resultSet = prep.executeQuery()
      
      val df = sqlContext.read.parquet("seunjeonResult.parquet")
      df.registerTempTable("result")
      while (resultSet.next) {
        val baseDf = sqlContext.sql(getSingleUrlContentsQuery + "'" + resultSet.getString("url") + "'").toDF()
        normalSentense ++ baseDf.map(elem => elem.get(0) + " ").reduce(_ + _)
      }

    } finally {
      mySqlConn.close

    }
    
    try {
      val prep = mySqlConn.prepareStatement(distinctQuery + "'2'")
      val resultSet = prep.executeQuery()
      
      val df = sqlContext.read.parquet("spamResult.parquet")
      df.registerTempTable("result")
      while (resultSet.next) {
        val baseDf = sqlContext.sql(getSingleUrlContentsQuery + "'" + resultSet.getString("url") + "'").toDF()
        spamSentense ++ baseDf.map(elem => elem.get(0) + " ").reduce(_ + _)
      }

    } finally {
      mySqlConn.close

    }

    val naiveBayesExample = new NaiveBayesExample
    naiveBayesExample.runNaiveBayesModel(sc, normalSentense, spamSentense)

    /*
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
*/

    sc.stop()
  }

}