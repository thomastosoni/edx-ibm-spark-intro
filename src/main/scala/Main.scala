import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object Main {
  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + " nanoseconds")
    result
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Samples").setMaster("local")
    val sc = SparkContext.getOrCreate(conf)
//    val spark = SparkSession.builder
//      .appName("Simple Application")
//      .config("spark.master", "local")
//      .config("spark.some.config.option", "some-value")
//      .getOrCreate()

    val xRangeRDD = sc.parallelize(1 to 50000, 4)
    xRangeRDD.cache()

    time { xRangeRDD.count() }
    time { xRangeRDD.count() }
  }
}
