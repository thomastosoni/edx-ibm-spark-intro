import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
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
  }
}
