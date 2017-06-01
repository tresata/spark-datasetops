package com.tresata.spark.datasetops

import org.apache.spark.sql.{ Dataset, Encoder, TypedColumn }
import org.apache.spark.sql.expressions.Aggregator

object `package` {
  private def aggregatorFromFunctions[A, B, C](zero: () => B, reduce: (B, A) => B, merge: (B, B) => B, finish: B => C)(implicit encB: Encoder[B], encC: Encoder[C]): Aggregator[A, B, C] = {
    val (z, r, m, f) = (zero, reduce, merge, finish)
    new Aggregator[A, B, C]{
      def zero: B = z()

      def reduce(b: B, a: A): B = r(b, a)

      def merge(b1: B, b2: B): B = m(b1, b2)

      def finish(b: B): C = f(b)

      def bufferEncoder: Encoder[B] = encB

      def outputEncoder: Encoder[C] = encC
    }
  }

  implicit class RichPairDataset[K, V](val ds: Dataset[(K, V)]) extends AnyVal {
    private[datasetops] def withDefaultFieldNames(implicit encKV: Encoder[(K, V)]): Dataset[(K, V)] = ds.toDF("_1", "_2").as[(K, V)]

    def mapValues[U](f: V => U)(implicit encKU: Encoder[(K, U)]): Dataset[(K, U)] = ds.map{ kv => (kv._1, f(kv._2)) }

    def reduceByKey(f: (V, V) => V)(implicit encK: Encoder[K], encV: Encoder[V]): Dataset[(K, V)] = ds.groupByKey(_._1).mapValues(_._2).reduceGroups(f)

    def aggByKey[U1](col1: TypedColumn[V, U1])(implicit encK: Encoder[K], encV: Encoder[V]): Dataset[(K, U1)] =
      ds.groupByKey(_._1).mapValues(_._2).agg(col1)

    def aggByKey[U1, U2](col1: TypedColumn[V, U1], col2: TypedColumn[V, U2])(implicit encK: Encoder[K], encV: Encoder[V]): Dataset[(K, U1, U2)] =
      ds.groupByKey(_._1).mapValues(_._2).agg(col1, col2)

    def aggByKey[U1, U2, U3](col1: TypedColumn[V, U1], col2: TypedColumn[V, U2], col3: TypedColumn[V, U3])(implicit encK: Encoder[K], encV: Encoder[V]): Dataset[(K, U1, U2, U3)] =
      ds.groupByKey(_._1).mapValues(_._2).agg(col1, col2, col3)

    def aggByKey[U1, U2, U3, U4](col1: TypedColumn[V, U1], col2: TypedColumn[V, U2], col3: TypedColumn[V, U3], col4: TypedColumn[V, U4])(implicit encK: Encoder[K], encV: Encoder[V]):
        Dataset[(K, U1, U2, U3, U4)] =
      ds.groupByKey(_._1).mapValues(_._2).agg(col1, col2, col3, col4)

    def aggByKey[B, U](zero: () => B, reduce: (B, V) => B, merge: (B, B) => B, finish: B => U)(implicit envK: Encoder[K], envV: Encoder[V], encB: Encoder[B], encU: Encoder[U]): Dataset[(K, U)] = {
      val sqlAgg = aggregatorFromFunctions(zero, reduce, merge, finish)
      ds.groupByKey(_._1).mapValues(_._2).agg(sqlAgg.toColumn)
    }

    def aggByKey[B, U](zero: B, reduce: (B, V) => B, merge: (B, B) => B, finish: B => U)(implicit envK: Encoder[K], envV: Encoder[V], encB: Encoder[B], encU: Encoder[U]): Dataset[(K, U)] =
      aggByKey(() => zero, reduce, merge, finish)

    def countByKey()(implicit encK: Encoder[K]): Dataset[(K, Long)] = ds.groupByKey(_._1).count

    def joinOnKey[V1](other: Dataset[(K, V1)])(implicit encKV: Encoder[(K, V)], encKV1: Encoder[(K, V1)], encKVV1: Encoder[(K, (V, V1))]): Dataset[(K, (V, V1))] = {
      val ds1 = ds.withDefaultFieldNames
      val other1 = other.withDefaultFieldNames
      ds1.joinWith(other1, ds1("_1") === other1("_1"), "inner").map{ x => (x._1._1, (x._1._2, x._2._2)) }
    }

    def leftOuterJoinOnKey[V1](other: Dataset[(K, V1)])(implicit encKV: Encoder[(K, V)], encKV1: Encoder[(K, V1)], encKVOptV1: Encoder[(K, (V, Option[V1]))]): Dataset[(K, (V, Option[V1]))] = {
      val ds1 = ds.withDefaultFieldNames
      val other1 = other.withDefaultFieldNames
      ds1.joinWith(other1, ds1("_1") === other1("_1"), "left_outer").map{ x => (x._1._1, (x._1._2, Option(x._2).map(_._2))) }
    }

    def rightOuterJoinOnKey[V1](other: Dataset[(K, V1)])(implicit encKV: Encoder[(K, V)], encKV1: Encoder[(K, V1)], encKVOptV1: Encoder[(K, (Option[V], V1))]): Dataset[(K, (Option[V], V1))] = {
      val ds1 = ds.withDefaultFieldNames
      val other1 = other.withDefaultFieldNames
      ds1.joinWith(other1, ds1("_1") === other1("_1"), "right_outer").map{ x => (x._2._1, (Option(x._1).map(_._2), x._2._2)) }
    }

    def fullOuterJoinOnKey[V1](other: Dataset[(K, V1)])(implicit encKV: Encoder[(K, V)], encKV1: Encoder[(K, V1)], encKVOptV1: Encoder[(K, (Option[V], Option[V1]))]):
        Dataset[(K, (Option[V], Option[V1]))] = {
      val ds1 = ds.withDefaultFieldNames
      val other1 = other.withDefaultFieldNames
      ds1.joinWith(other1, ds1("_1") === other1("_1"), "full_outer").map{ x => (if (x._1 == null) x._2._1 else x._1._1, (Option(x._1).map(_._2), Option(x._2).map(_._2))) }
    }

    def keys(implicit encK: Encoder[K]): Dataset[K] = ds.map(_._1)

    def values(implicit encV: Encoder[V]): Dataset[V] = ds.map(_._2)

    def partitionByKey(numPartitions: Int)(implicit encKV: Encoder[(K, V)]): Dataset[(K, V)] = {
      val ds1 = ds.withDefaultFieldNames
      ds1.repartition(numPartitions, ds1("_1"))
    }

    def partitionByKey(implicit encKV: Encoder[(K, V)]): Dataset[(K, V)] = {
      val ds1 = ds.withDefaultFieldNames
      ds1.repartition(ds1("_1"))
    }

    def sortWithinPartitionsByKey(implicit encKV: Encoder[(K, V)]): Dataset[(K, V)] = {
      val ds1 = ds.withDefaultFieldNames
      ds1.sortWithinPartitions(ds1("_1"))
    }
  }
}
