import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * spark 1.x的创建dataframe 方案
 * 利用SQLContext来编程(结合structType来创建DataFrame)
 * 1.先创建SparkContext,再创建SQLContext
 * 2. 先创建RDD,将读取到的数据转为Row
 * 3. 创建StructType,用于指定数据的schema
 * 4. 通过sqlContext创建DataFrame,并指定schema
 * 5. 注册临时表
 * 6. 执行SQL( 这种SQL都是 Trasformation,lazy操作)
 * 7.执行Action
 */
object Test2_SQLContext {
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
    val schema = StructType(   List(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true),
      StructField("height", DoubleType, true)
    ))

    val df=sqlContext.createDataFrame(   personRDD,   schema )   // MVC

    //得到DataFrame后，有两种操作：1。 SQL   2。 DataFrame的API( DSL )
    df.registerTempTable("t_person")
    val resultDataFrame = sqlContext.sql("select * from t_person order by age desc,height desc, name asc")
    resultDataFrame.show()
    sc.stop()
  }
}
