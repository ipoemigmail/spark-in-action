package sparkinaction.ch08

import org.apache.spark._
import org.apache.spark.ml.feature.OneHotEncoderEstimator
import org.apache.spark.sql._
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.feature.VectorAssembler

import scala.util.Try

object Ch08 {

  val adultSchema: StructType = StructType(
    Array(
      StructField("age", DoubleType, true),
      StructField("workclass", StringType, true),
      StructField("fnlwgt", DoubleType, true),
      StructField("education", StringType, true),
      StructField("marital_status", StringType, true),
      StructField("occupation", StringType, true),
      StructField("relationship", StringType, true),
      StructField("race", StringType, true),
      StructField("sex", StringType, true),
      StructField("capital_gain", DoubleType, true),
      StructField("capital_loss", DoubleType, true),
      StructField("hours_per_week", DoubleType, true),
      StructField("native_country", StringType, true),
      StructField("income", StringType, true)
    )
  )

  def indexStringColumns(df: DataFrame, cols: Array[String]): DataFrame = {
    import org.apache.spark.ml.feature.StringIndexer
    import org.apache.spark.ml.feature.StringIndexerModel

    cols.foldLeft(df) { (df1, c) =>
      val si = new StringIndexer().setInputCol(c).setOutputCol(c + "-num")
      val sm = si.fit(df1)
      sm.transform(df1).drop(c).withColumnRenamed(c + "-num", c)
    }
  }

  def oneHotEncodeColumns(df: DataFrame, cols: Array[String]): DataFrame = {
    import org.apache.spark.ml.feature.OneHotEncoder
    val onehotenc =
      new OneHotEncoderEstimator().setInputCols(cols).setOutputCols(cols.map(_ + "-onehot")).setDropLast(false)
    cols.foldLeft(onehotenc.fit(df).transform(df)) { (df1, c) =>
      df1.drop(c).withColumnRenamed(c + "-onehot", c)
    }
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local[*]").appName("ammonite-local").getOrCreate

    def sc = spark.sparkContext

    import spark.implicits._

    val censusRaw = sc
      .textFile("ch08/adult.raw", 4)
      .map(x => x.split(",").map(_.trim))
      .map(row => row.map(x => Try(x.toDouble).getOrElse(x)))

    val dfraw = spark.createDataFrame(censusRaw.map(Row.fromSeq(_)), adultSchema)
    val dfrawrp = dfraw.na.replace(Array("workclass"), Map("?" -> "Private"))
    val dfrawrpl = dfrawrp.na.replace(Array("occupation"), Map("?" -> "Prof-specialty"))
    val dfrawnona = dfrawrpl.na.replace(Array("native_country"), Map("?" -> "United-States"))

    val dfnumeric = indexStringColumns(
      dfrawnona,
      Array(
        "workclass",
        "education",
        "marital_status",
        "occupation",
        "relationship",
        "race",
        "sex",
        "native_country",
        "income"
      )
    )

    val dfhot = oneHotEncodeColumns(
      dfnumeric,
      Array(
        "workclass",
        "education",
        "marital_status",
        "occupation",
        "relationship",
        "race",
        "native_country"
      )
    )

    val va = new VectorAssembler().setOutputCol("features")
    va.setInputCols(dfhot.columns.diff(Array("income")))
    val lpoints = va.transform(dfhot).select("features", "income").withColumnRenamed("income", "label")

    dfrawnona.show(truncate = false)
    dfnumeric.show(truncate = false)
    dfhot.show(truncate = false)
    lpoints.show(truncate = false)
  }

}
