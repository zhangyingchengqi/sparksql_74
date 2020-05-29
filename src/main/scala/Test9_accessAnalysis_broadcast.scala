import org.apache.spark.sql.SparkSession
import utils.{IpRule, YcUtil}

//解决的问题:   1. 环境配置 (   maven 刷新  )   2. scala版本
object Test9_accessAnalysis_broadcast {
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
      new IpRule(startNum, endNum, province) //    注意：   Dataset[样例类对象] ->  toDF(   )  对比以前:    RDD[Row]-> StructType转换成DataFrame
    })
    val ipDataFrame = ipDataset.toDF("startNum", "endNum", "province")
    //发布成广播变量
    val ipRulesArray = ipDataFrame.collect()
    val broadcastRef = spark.sparkContext.broadcast(ipRulesArray)

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
    accessIpLongDataFrame.createTempView("v_accessIpLong") //  条件
    //自定义函数   :   sql 自定义函数
    // hive自定义函数：   UDF  1V1  to_date，   UDA：  nV1  sum,avg，     UDTF :  1Vn  pivot_table
    spark.udf.register("ip2province", (ipNum: Long) => {
      //通过广播变量取出  ipRulesArray
      val ipRulesArray = broadcastRef.value
      var province = "unkown"
      var index = YcUtil.binarySearch(ipRulesArray, ipNum)
      if (index != -1) {
        province = ipRulesArray(index).getAs("province")
      }
      province
    })

    //val resultDataFrame=spark.sql("select ip2province(ipNum) province, count(*) cn from v_accessIpLong  group by province  ")

    //方案二:  2. DSL
    //  如果在查询的列中要调用自定义的函数，使用DSL的话，则要用  selectExpr 方法.
    // 方法签名:  ds.selectExpr("colA", "colB as newName", "abs(colC)")
    //   select ip2province(ipNum) province, count(*) cn from v_accessIpLong  group by province
    //  $"ip2province(ipNum)".as("province").toString(): 计算列转换名字
    val resultDataFrame=accessIpLongDataFrame.selectExpr(   $"ip2province(ipNum)".as("province").toString() ).groupBy(   $"province").count()

    resultDataFrame.show()

    spark.stop()

  }
}


