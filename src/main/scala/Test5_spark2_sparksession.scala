import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * spark 2.x的创建dataframe 方案
 *
 */
object Test5_spark2_sparksession {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local[*]")
      .getOrCreate()

    //val lines :Dataset[String]= spark.read.textFile("data/person.txt") //    Dataset[Row]
    val lines = spark.sparkContext.textFile("data/person.txt") //  RDD[String]

    // map 一下,生成对象
    val personRDD = lines.map(line => {
      val fields = line.split(",")
      val id = fields(0).toInt
      val name = fields(1)
      val age = fields(2).toInt
      val height = fields(3).toDouble
      Row(id, name, age, height)
    })
    //   schema的创建    相当于前面   case class的作用
    val schema = StructType(List(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true),
      StructField("height", DoubleType, true)
    ))
    // spark 2.x 用sparkSession创建DataFrame
    val df: DataFrame = spark.createDataFrame(personRDD, schema)
    // spark 1.用SQlContext创建DataFrame
    //val df:DataFrame=sqlContext.createDataFrame(   personRDD,   schema )   // MVC

    //得到DataFrame后，有两种操作：1。 SQL   2。 DataFrame的API( DSL )
    // 1. sql
    // df.registerTempTable("t_person")
    // val resultDataFrame = spark.sql("select * from t_person order by age desc,height desc, name asc")

    //2. api(DSL   ->  $字段名转为ColumnName对象，需要隐式转换
    //  为什么是  spark.implicits._ 而不是 import sqlContext.implicits._
    import spark.implicits._
    val resultDataFrame = df.select("id", "name", "age").where($"age" > 10)
               .orderBy($"age" desc).orderBy($"height" desc).orderBy($"name" asc)

    resultDataFrame.show()
    spark.stop()
  }
}
