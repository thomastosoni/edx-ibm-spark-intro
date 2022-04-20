import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object Module5 {

  def module5(): Unit = {
    val conf = new SparkConf()
      .setAppName("Module 5")
      .setMaster("spark://localhost:7077")

    val sc = SparkContext.getOrCreate(conf)
    sc.setLogLevel("INFO")

    val spark = SparkSession.builder.getOrCreate()

    val list = java.util.Arrays.asList(Row(1, "foo"), Row(2, "bar"))

    val df = spark.createDataFrame(
      rows = list,
      schema = StructType(
        Array(
          StructField(name = "id", dataType = IntegerType, nullable = false),
          StructField(name = "txt", dataType = StringType, nullable = false),
        )
      )
    )

    print(df)
    df.show()
  }
}
