package com.epam.training.spark.sql

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.internal.util.TableDef.Column

object Homework {
  val DELIMITER = ";"
  val RAW_BUDAPEST_DATA = "data/budapest_daily_1901-2010.csv"
  val OUTPUT_DUR = "output"

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
      .setAppName("EPAM BigData training Spark SQL homework")
      .setIfMissing("spark.master", "local[2]")
      .setIfMissing("spark.sql.shuffle.partitions", "10")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    processData(sqlContext)

    sc.stop()

  }

  def processData(sqlContext: HiveContext): Unit = {

    /**
      * Task 1
      * Read csv data with DataSource API from provided file
      * Hint: schema is in the Constants object
      */
    val climateDataFrame: DataFrame = readCsvData(sqlContext, Homework.RAW_BUDAPEST_DATA)

    /**
      * Task 2
      * Find errors or missing values in the data
      * Hint: try to use udf for the null check
      */
    val errors: Array[Row] = findErrors(climateDataFrame)
    println(errors)

    /**
      * Task 3
      * List average temperature for a given day in every year
      */
    val averageTemeperatureDataFrame: DataFrame = averageTemperature(climateDataFrame, 1, 2)

    /**
      * Task 4
      * Predict temperature based on mean temperature for every year including 1 day before and after
      * For the given month 1 and day 2 (2nd January) include days 1st January and 3rd January in the calculation
      * Hint: if the dataframe contains a single row with a single double value you can get the double like this "df.first().getDouble(0)"
      */
    val predictedTemperature: Double = predictTemperature(climateDataFrame, 1, 2)
    println(s"Predicted temperature: $predictedTemperature")

  }

  def readCsvData(sqlContext: HiveContext, rawDataPath: String): DataFrame = {
    sqlContext.read
      .option("header", "true")
      .option("delimiter", ";")
      .schema(getSchema)
      .csv(rawDataPath)
  }

  def findErrors(climateDataFrame: DataFrame): Array[Row] =
    climateDataFrame.agg(
      sum(when(climateDataFrame("observation_date").isNull, 1).otherwise(0)),
      sum(when(climateDataFrame("mean_temperature").isNull, 1).otherwise(0)),
      sum(when(climateDataFrame("max_temperature").isNull, 1).otherwise(0)),
      sum(when(climateDataFrame("min_temperature").isNull, 1).otherwise(0)),
      sum(when(climateDataFrame("precipitation_mm").isNull, 1).otherwise(0)),
      sum(when(climateDataFrame("precipitation_type").isNull, 1).otherwise(0)),
      sum(when(climateDataFrame("sunshine_hours").isNull, 1).otherwise(0))).collect()

  def averageTemperature(climateDataFrame: DataFrame, monthNumber: Int, dayOfMonth: Int): DataFrame =
    climateDataFrame
      .filter(month(col("observation_date")) === monthNumber &&
              dayofmonth(col("observation_date")) === dayOfMonth)
      .select(col("mean_temperature"))

  def predictTemperature(climateDataFrame: DataFrame, monthNumber: Int, dayOfMonth: Int): Double =
    climateDataFrame
      .filter((month(col("observation_date")) === monthNumber &&
                dayofmonth(col("observation_date")) === dayOfMonth) ||
              (month(date_add(col("observation_date"), 1)) === monthNumber &&
                dayofmonth(date_add(col("observation_date"), 1)) === dayOfMonth) ||
              (month(date_sub(col("observation_date"), 1)) === monthNumber &&
                dayofmonth(date_sub(col("observation_date"), 1)) === dayOfMonth))
      .agg(avg(col("mean_temperature")))
      .collect()(0)
      .getDouble(0)

  private def getSchema: StructType =
    StructType(
      Array(
        StructField("date", DateType), StructField("mean_temp", DoubleType), StructField("max_temp", DoubleType),
        StructField("min_temp", DoubleType), StructField("perc_amnt", DoubleType), StructField("perc_type", DoubleType),
        StructField("sunshine_hours", DoubleType)))

  private def getMissingDataIndicator(value: Any) : Int =
    value match {
      case s:String => if (s.isEmpty) 0 else 1
      case _ => 0
    }
}


