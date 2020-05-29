import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * spark 1.x的创建dataframe 方案
 *
 */
object Test4_SQLContext_structType_api {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("spark sql one").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //创建SQLContext对象
    val sqlContext = new SQLContext(sc) //  源代码:    this(SparkSession.builder().sparkContext(sc).getOrCreate())
    val lines = sc.textFile("data/person.txt") //以前读取txt为行的方案
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

    val df = sqlContext.createDataFrame(personRDD, schema) // MVC

    //得到DataFrame后，有两种操作：1。 SQL   2。 DataFrame的API( DSL )
    //2. DataFrame的Api
    import sqlContext.implicits._
    val resultDataFrame = df.select("id", "name", "age", "height").orderBy($"age" desc, $"height" desc, $"name".asc)
    resultDataFrame.show()

    sc.stop()
  }
}
