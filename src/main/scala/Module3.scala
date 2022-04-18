import org.apache.spark.sql.SparkSession

object Module3 {
  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + " nanoseconds")
    result
  }

  def module3(): Unit = {
    //    val conf = new SparkConf().setAppName("Samples").setMaster("local")
    //    val sc = SparkContext.getOrCreate(conf)
    //
    //    val xRangeRDD = sc.parallelize(1 to 50000, 4)
    //    xRangeRDD.cache()
    //
    //    time { xRangeRDD.count() }
    //    time { xRangeRDD.count() }

    val spark = SparkSession.builder
      .appName("Simple Application")
      .config("spark.master", "local")
      .getOrCreate()

    val df = spark.read.json("src/main/resources/people.json").cache()
    df.show()
    df.printSchema()

    df.createTempView("people")

    df.select("name").show()
    df.select(df("name")).show()
    spark.sql("SELECT name FROM people").show()

    df.filter(df("age") > 21).show()
    spark.sql("SELECT age, name FROM people WHERE age > 21").show()

    df.groupBy("age").count().show()
    spark.sql("SELECT age, COUNT(age) as count FROM people GROUP BY age").show()

    spark.close()
  }
}
