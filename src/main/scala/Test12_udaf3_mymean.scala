import org.apache.spark.sql.{Encoder, Encoders, Row, SparkSession}
import org.apache.spark.sql.expressions.Aggregator

/**
 * 继承Aggregator这个类，优点是可以带类型
 */
object Test12_udaf3_mymean {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("ip analysis")
      .master("local[*]")
      .getOrCreate()

    //生成数据 Dataset[Long]
    import spark.implicits._
    val range=spark.range(1,11)
    val r=range.map(   line=>{
      (  line.toLong )
    })
    r.show()
    //调用自定义的函数，完成计算
    //import spark.implicits._
    r.select( GeoMean2.toColumn.name("geomean")).show()

    spark.stop()
  }
}

object GeoMean2 extends Aggregator[Long, MiddleResult, Double] {
  // 初始化buffer
  override def zero: MiddleResult = {
    MiddleResult(1.0, 0L)
  }

  /*
  在一个分区上时，当传入一条新数据,由spark框架自动回调方法, 注入(   DI  )  两个参数
       MiddleResult: 存缓冲区
      Double: 一条数据
   */
  override def reduce(b: MiddleResult, a: Long): MiddleResult = {
    b.product = b.product * a
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
    println(  reduction.product+"   "+ reduction.count )
    math.pow(reduction.product, 1.toDouble / reduction.count)
  }

  override def bufferEncoder: Encoder[MiddleResult] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

//中间结果的buffer
//case class MiddleResult(var product: Double, var count: Long) {
//}

