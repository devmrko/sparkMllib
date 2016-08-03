import org.apache.spark.SparkContext

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