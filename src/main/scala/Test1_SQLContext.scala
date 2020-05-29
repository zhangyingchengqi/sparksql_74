import org.apache.spark.sql.SQLContext     /// 类
import org.apache.spark.{SparkConf, SparkContext}

/**
 * spark 1.x的创建dataframe 方案
 * 利用SQLContext来编程(结合case class来创建DataFrame)
 * 1.先创建SparkContext,再创建SQLContext
 * 2. 先创建RDD,对数据进行整理，然后关联case class,将非结构化数据转换为结构化数据
 * 3. 显示调用toDF将RDD转为DataFrame
 * 4. 注册临时表
 * 5. 执行SQL( 这种SQL都是 Trasformation,lazy操作)
 * 6.执行Action
 */
object Test1_SQLContext {
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
      //转成对象
      Person(id, name, age, height) // -> apply()
    })
    //引入隐式转换，将rdd转为   DataFrame
    import sqlContext.implicits._
    val df = personRDD.toDF()
    df.show()
    //得到DataFrame后，有两种操作：1。 SQL   2。 API
    df.registerTempTable("t_person")
    val resultDataFrame=sqlContext.sql("select * from t_person order by age desc,height desc, name asc")
    resultDataFrame.show()
    sc.stop()
  }
}
case class Person(id: Integer, name: String, age: Integer, height: Double) //  利用反射读取样例类对象的信息

//   class             object( 静态，单例)
