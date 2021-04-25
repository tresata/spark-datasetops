package com.tresata.spark.datasetops

import org.scalactic.Equality
import org.apache.spark.SparkContext
import org.apache.spark.sql.{ Dataset, SparkSession }

object SparkSuite {
  lazy val spark: SparkSession = {
    val session = SparkSession.builder
      .master("local[*]")
      .appName("test")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.ui.enabled", false)
      .config("spark.sql.shuffle.partitions", 4)
      .getOrCreate()
    session
  }
  lazy val sc: SparkContext = spark.sparkContext
}

trait SparkSuite {
  implicit lazy val spark: SparkSession = SparkSuite.spark

  implicit def dsEq[X]: Equality[Dataset[X]] = new Equality[Dataset[X]] {
    private def toCounts[Y](s: Seq[Y]): Map[Y, Int] = s.groupBy(identity).mapValues(_.size).toMap

    def areEqual(a: Dataset[X], b: Any): Boolean = b match {
      case d: Dataset[_] => toCounts(a.collect()) == toCounts(d.collect())
      case s: Seq[_] => toCounts(a.collect()) == toCounts(s)
    }
  }
}
