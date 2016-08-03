import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object MySqlApp {

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

    var readJsonFileInfo = "input/spam"
    val jsonRDD = sc.textFile(readJsonFileInfo)

    val mySqlExample = new MySqlExample
    mySqlExample.runMySql(sc)

    sc.stop()
  }

}

class MySqlExample {

  def runMySql(sc: SparkContext) {
    
    val mysqlUrl = "jdbc:mysql://192.168.10.33/master";
    val tableName = "stopwords"
    val userName = "hadoop"
    val passwd = "hadoop"

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val dataframe_mysql = sqlContext.read.format("jdbc")
      .option("url", mysqlUrl)
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", tableName)
      .option("user", userName)
      .option("password", passwd)
      .load()

    dataframe_mysql.registerTempTable("stopwords")
    //    dataframe_mysql.show()
    dataframe_mysql.sqlContext.sql("select name from stopwords").collect.foreach(println)
    
  }

}