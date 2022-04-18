import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{DoubleType, StringType, StructType}

object Module4 {
  case class MTCar(
    name: String,
    mpg: Double,
    cyl: Double,
    disp: Double,
    hp: Double,
    drat: Double,
    wt: Double,
    qsec: Double,
    vs: Double,
    am: Double,
    gear: Double,
    carb: Double
  )

  def module4(): Unit = {
    val spark = SparkSession.builder
      .appName("Simple Application")
      .config("spark.master", "local")
      .getOrCreate()

    val schema = new StructType()
      .add(name = "name", dataType = StringType, nullable = false)
      .add(name = "mpg", dataType = DoubleType, nullable = false)
      .add(name = "cyl", dataType = DoubleType, nullable = false)
      .add(name = "disp", dataType = DoubleType, nullable = false)
      .add(name = "hp", dataType = DoubleType, nullable = false)
      .add(name = "drat", dataType = DoubleType, nullable = false)
      .add(name = "wt", dataType = DoubleType, nullable = false)
      .add(name = "qsec", dataType = DoubleType, nullable = false)
      .add(name = "vs", dataType = DoubleType, nullable = false)
      .add(name = "am", dataType = DoubleType, nullable = false)
      .add(name = "gear", dataType = DoubleType, nullable = false)
      .add(name = "carb", dataType = DoubleType, nullable = false)

    val sdf = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      //      .schema(schema)
      .load("src/main/resources/mtcars.csv")
      .withColumnRenamed("_c0","name")
      .cache()

    sdf.printSchema()
    sdf.show(5)
    sdf.select("mpg").show(5)
    sdf.filter(sdf("mpg") < 18).show(5)
    sdf.withColumn("wtTon", sdf("wt") * 0.45).show(5)

    // Aggregations
    sdf.groupBy("cyl")
      .agg(Map("wt" -> "AVG"))
      .show(5)

    sdf.groupBy("cyl")
      .agg(Map("wt" -> "COUNT"))
      .sort(col("count(wt)").desc)
      .show(5)

    // SparkSQL
    sdf.createTempView("cars")

    spark.sql("SELECT * FROM cars").show()
    spark.sql("SELECT mpg FROM cars").show(5)
    spark.sql("SELECT * FROM cars where mpg>20 AND cyl < 6").show(5)
    spark.sql("SELECT count(*), cyl from cars GROUP BY cyl").show()
    spark.sql("SELECT * FROM cars where name like 'Merc%'").show()

    // udf
    val convertWt = udf((s: Float) => 0.45 * s)
    spark.udf.register("convert_weight", convertWt)
    spark.sql("SELECT *, wt AS weight_imperial, convert_weight(wt) as weight_metric FROM cars").show()
  }
}
