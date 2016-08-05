import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.sql.{ Connection, DriverManager, ResultSet };

object HDFSToMySQLApp {

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

    val df = sqlContext.read.parquet("spamResult.parquet")
    df.registerTempTable("result")

    var query = "SELECT	name, type, count(*) "
    query += "FROM result "
    query += "where type not in ('SF', 'SN', 'JKB', 'EC', 'ETM', 'JKS', 'JX', 'JKO', 'JKG', 'SSO') "
    //    query += "and type = 'NNG' "
    query += "group by name, type "
    query += "order by count(*) desc "
    query += "limit 500 "

    var distinctQuery = "SELECT	distinct url FROM result "

    val mysqlUrl = "jdbc:mysql://192.168.10.33/master";
    val userName = "hadoop"
    val passwd = "hadoop"

    //    val prop = new java.util.Properties
    //    prop.setProperty("user", userName)
    //    prop.setProperty("password", passwd)

    //    val blogContentsInfoTable = sqlContext.read.jdbc(mysqlUrl, "blogContentsInfo", prop)
    //    val stopwordsTable = sqlContext.read.jdbc(mysqlUrl, "stopwords", Array("del_yn='N'"), prop)
    //    stopwordsTable.foreach(println)

    val mySqlConn = DriverManager.getConnection(mysqlUrl + "?user=" + userName + "&password=" + passwd)
    val statement = mySqlConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)

    try {
      sqlContext.sql(distinctQuery).collect().foreach(x => {
        val queryStmt = "INSERT INTO blogContentsInfo (job_id, url, reg_id, reg_date) VALUES (?, ?, ?, now()) "
        val prep = mySqlConn.prepareStatement(queryStmt)
        prep.setString(1, "2")
        prep.setString(2, x.toString().replace("[", "").replace("]", ""))
        prep.setString(3, "system")
        prep.executeUpdate
      })

    } finally {
      mySqlConn.close
      
    }

    sc.stop()
  }

}