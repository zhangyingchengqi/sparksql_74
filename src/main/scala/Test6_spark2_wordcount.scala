import org.apache.spark.sql.{Dataset, SparkSession}

object Test6_spark2_wordcount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL wordcount")
      .master("local[*]")
      .getOrCreate()

    val lines :Dataset[String]= spark.read.textFile("data/wc.txt") //    Dataset[Row]   此时Row中只有一个列

    //val lines = spark.sparkContext.textFile("data/wc.txt") //  RDD[String]

    import spark.implicits._
    val words=lines.flatMap(   _.split(" "))   // DataSet
    // DataSet就是另类的   DataFrame
    //方案一:   sql
   // words.createTempView("wc")
    //val result= spark.sql("select value, count(*) nums from wc group by value order by nums desc")

    //方案二:  Api
    val result=words.groupBy($"value" as "word").count().sort($"count" desc)

    result.show()
    spark.stop()


  }
}
