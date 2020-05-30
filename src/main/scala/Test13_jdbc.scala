import java.util.Properties

import org.apache.spark.sql.SparkSession

object Test13_jdbc {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("ip analysis")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    //  JSON/CSV/PARQuet/jdbc
    val resultDF = spark.read.format("jdbc")
      .options(Map("url" -> "jdbc:mysql://localhost:3306/bigdata", "driver" -> "com.mysql.jdbc.Driver", "user" -> "root", "password" -> "a", "dbtable" -> "result"))
      .load()

    //表结构
    println("输出表结构:")
    resultDF.printSchema()
    println("输出表内容( top 20 )")
    resultDF.show()
    //对表中的内容进行操作的话，有两种方式，:
    //1. sql
    //2. api
    println("3. 以lambda(DSL) 过滤:")
    val filtered = resultDF.filter($"nums" > 1000)
    filtered.show()
    println("4.以where函数加入DSL过滤")
    val whereed = resultDF.where($"nums" > 1000)
    whereed.show()
    println("5. select函数指定返回的列(指定别名，运算) ")
    val selected = resultDF.select($"id", $"occ", $"mtype", $"nums" * 10 as "changednums")
    selected.show()
    //把结果保存到数据库
    println("6. 结果保存到数据库")

    val props = new Properties()
    props.put("user", "root")
    props.put("password", "a")
    // Specifies the behavior when data or table already exists.
    // Options include:
    // - overwrite: overwrite the existing data.
    // - append: append the data.
    // - ignore: ignore the operation (i.e. no-op).
    // - error: default option, throw an exception at runtime.
    selected.write.mode("ignore").jdbc("jdbc:mysql://localhost:3306/bigdata", "result3", props)
    println("保存到mysql成功")

    println("7.保存到txt")
    //异常:Text data source supports only a single column, and you have 4 columns
    // 如何存成文本，只允许一个列，但这里有四个列
    //selected.write.mode("append").text("data/txtResult")      // hadoop:  分区有多个，对应的输出文件就有多个
    //解决方案: 用map合并多个列为一个列，或转类型
    selected.toJavaRDD.saveAsTextFile("data/txtResult")

    println("8.保存到json")
    selected.write.mode("append").json("data/jsonResult")

    println("9.保存到csv")
    selected.write.mode("append").csv("data/csvResult")

    println("10.保存到parquet")
    selected.write.mode("append").parquet("data/parquetResult")


    spark.stop()
  }

}
