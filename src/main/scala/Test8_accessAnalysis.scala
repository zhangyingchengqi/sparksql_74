import org.apache.spark.sql.{Row, SparkSession}
import utils.YcUtil

//解决的问题:   1. 环境配置 (   maven 刷新  )   2. scala版本
object Test8_accessAnalysis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("ip analysis")
      .master("local[*]")
      .getOrCreate()


    import spark.implicits._
    val ipLinesDataset = spark.read.textFile("data/ip.txt") // Dataset 只有一列  value:   1.0.1.0|1.0.3.255|16777472|16778239|亚洲|中国|福建|福州||电信|350100|China|CN|119.306239|26.075302
    val ipDataset = ipLinesDataset.map(line => {
      val fields = line.split("\\|")
      val startNum = fields(2).toLong
      val endNum = fields(3).toLong
      val province = fields(6).toString
      (startNum, endNum, province)    //    注意：   Dataset[元组] ->  toDF(列名)  对比以前:    RDD[Row]-> StructType转换成DataFrame
    })
    val ipDataFrame = ipDataset.toDF("startNum", "endNum", "province")
    //ipDataFrame.show()

    val accessDataset = spark.read.textFile("data/access.log")
    val accessIpLongDataFrame = accessDataset.map(line => {
      val fields = line.split("\\|")
      val ip = fields(1) //字符串
      val ipNum = YcUtil.ip2Long(ip)
      ipNum
    }).toDF("ipNum")
    //accessIpLongDataFrame.show()

    //方案一: SQL方式
    //1. 临时表或视图
    //ipDataFrame.createTempView("v_ip")
   // accessIpLongDataFrame.createTempView("v_accessIpLong")  //  条件
    //val resultDataFrame=spark.sql("select province, count(*) cn from  v_accessIpLong inner join v_ip on (ipNum>=startNum and ipNum<=endNum) group by province  ")


    //方案二:  API+DSL
    val resultDataFrame=ipDataFrame.join(  accessIpLongDataFrame,  $"ipNum">=$"startNum" and $"ipNum"<=$"endNum"  ).groupBy(  "province" ).count()

    resultDataFrame.show()

    spark.stop()

  }
}
