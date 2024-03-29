import org.apache.spark.sql.SparkSession

/**
 * dataframe中的常见函数汇总
 */
object Test14_dataframe_detial {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("ip analysis")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    //load这个方法会读取真正mysql的数据吗？
    val dataFrame = spark.read.format("jdbc").options(
      Map("url" -> "jdbc:mysql://localhost:3306/bigdata",
        "driver" -> "com.mysql.jdbc.Driver",
        "dbtable" -> "students",
        "user" -> "root",
        "password" -> "a")
    ).load()
    dataFrame.show()
    //1. 将dataFrame注册为临时表，后续用sql进行查询
    dataFrame.registerTempTable("students")

    // API方案
    //1. head( num)
    dataFrame.head(3).foreach(println)
    //2. show( num )
    dataFrame.show(3)

    //常用列操作
    //1. columns
    println(dataFrame.columns)
    //2. dtypes
    println(dataFrame.dtypes)
    //3. printSchema
    dataFrame.printSchema()
    //4. select
    dataFrame.select("name", "age").show()    //直接取列
    dataFrame.select($"name"+"_", $"age"+1).show()   //加入运算
    //5. withColumn  -> 添加新列
    dataFrame.withColumn("ageAfter10", dataFrame("age") + 10).show()
    //6. withColumnRenamed(existing, new)   ->重命名,且返回新的DataFrame
    val newDataFrame = dataFrame.withColumnRenamed("age", "ageafter10")
    newDataFrame.show()

    //过滤
    //1. filter( condition)
    import spark.implicits._   //要支持  $  ,必须引入  implicits
    newDataFrame.filter($"ageafter10" > 35).show()
    //2. where(condition)
    newDataFrame.where($"ageafter10" > 35).show()

    //排序
    //1. orderBy( cols* )
    println( "排序" )
    newDataFrame.orderBy(   newDataFrame("ageafter10")   ).show(5)
    newDataFrame.orderBy("ageafter10", "id").show(5)
    newDataFrame.sort(    $"ageafter10".desc   ).show()

    //转换
    println("转换操作")
    newDataFrame.toJSON.collect().foreach(  println)
    //2. repartition
    println("将数据的分区数改为4")
    println(  newDataFrame.repartition(4).rdd.partitions.size  )

    spark.stop()
  }
}
