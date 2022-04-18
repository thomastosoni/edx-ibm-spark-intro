import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Samples").setMaster("local")
    val sc = SparkContext.getOrCreate(conf)
//    val spark = SparkSession.builder
//      .appName("Simple Application")
//      .config("spark.master", "local")
//      .config("spark.some.config.option", "some-value")
//      .getOrCreate()

    val data = (1 to 30)
    val xRangeRDD = sc.parallelize(data)

    val subRDD = xRangeRDD.map(_ - 1)
    val filteredRDD = subRDD.filter(_ < 10)

    println(filteredRDD.collect().mkString("Array(", ", ", ")"))
    println(filteredRDD.count())

//    spark.stop()
  }
}
