import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.apache.spark.sql.expressions.Aggregator

object Test11_udaf2_mymean {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("ip analysis")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val studentsDataset=spark.read.textFile("data/students.txt")
    val studentsDS = studentsDataset.map(line => {
      val fields = line.split(",")
      val id = fields(0).toInt
      val name = fields(1)
      val age = fields(2).toInt
      val height = fields(3).toDouble
      val cid = fields(4).toInt
      Student(id, name, age, height, cid)   //     一行
    })
    //调用自定义的函数，完成计算
    //import spark.implicits._
    studentsDS.select( GeoMean.toColumn.name("geomean")).show()

    spark.stop()
  }
}

object GeoMean extends Aggregator[Student, MiddleResult, Double] {
  // 初始化buffer
  override def zero: MiddleResult = {
    MiddleResult(1.0, 1L)
  }

  /*
  在一个分区上时，当传入一条新数据,由spark框架自动回调方法, 注入(   DI  )  两个参数
       MiddleResult: 存缓冲区
      Double: 一条数据
   */
  override def reduce(b: MiddleResult, a: Student): MiddleResult = {
    b.product = b.product * a.height
    b.count = b.count + 1
    b
  }

  //不同分区的结果做全局聚合
  override def merge(b1: MiddleResult, b2: MiddleResult): MiddleResult = {
    b1.product = b1.product * b2.product
    b1.count = b1.count + b2.count
    b1
  }

  ///最终结果运算
  override def finish(reduction: MiddleResult): Double = {
    math.pow(reduction.product, 1.toDouble / reduction.count)
  }

  override def bufferEncoder: Encoder[MiddleResult] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

//中间结果的buffer
case class MiddleResult(var product: Double, var count: Long) {
}

case class Student(id: Integer, name: String, age: Integer, height: Double, cid: Integer)
