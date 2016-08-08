import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import java.sql.{ Connection, DriverManager, ResultSet };

class MySqlHelper {

  val mysqlUrl = "jdbc:mysql://192.168.10.33/master";
  val tableName = "stopwords"
  val userName = "hadoop"
  val passwd = "hadoop"

  def getDataframeFromTable(sqlContext: SQLContext, tableName: String): DataFrame = {
    val dataframe_mysql = sqlContext.read.format("jdbc")
      .option("url", mysqlUrl)
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", tableName)
      .option("user", userName)
      .option("password", passwd)
      .load()
    dataframe_mysql
  }
  
  def getMySqlConn(resultSetType: Int, resultSetConcurrency: Int): Connection = {
    val prop = new java.util.Properties
    prop.setProperty("user", userName)
    prop.setProperty("password", passwd)
    val conn = DriverManager.getConnection(mysqlUrl + "?user=" + userName + "&password=" + passwd)
    conn.createStatement(resultSetType, resultSetConcurrency)
    conn
  }

}