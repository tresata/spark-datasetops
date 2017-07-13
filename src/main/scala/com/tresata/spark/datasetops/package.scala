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

    /** Apply a provided function to the values of the key-value Dataset.
      *
      * {{{
scala> import com.tresata.spark.datasetops.RichPairDataset
scala> Seq((1, 2), (1, 3)).toDS.mapValues(_ + 2).show
+---+---+
| _1| _2|
+---+---+
|  1|  4|
|  1|  5|
+---+---+
      * }}}
      */
    def mapValues[U](f: V => U)(implicit encKU: Encoder[(K, U)]): Dataset[(K, U)] = ds.map{ kv => (kv._1, f(kv._2)) }

    /** Flat-map the key-value Dataset's values for each key, with the provided function.
      *
      * {{{
scala> import com.tresata.spark.datasetops.RichPairDataset
scala> Seq((1, 2)).toDS.flatMapValues{ case x => List(x, x + 1) }.show
+---+---+
| _1| _2|
+---+---+
|  1|  2|
|  1|  3|
+---+---+
      * }}}
      */
    def flatMapValues[U](f: V => TraversableOnce[U])(implicit encKU: Encoder[(K, U)]): Dataset[(K, U)] = ds.flatMap{ kv => f(kv._2).map(v1 => (kv._1, v1)) }

    /** Reduce the key-value Dataset's values for each key, with the provided function.
      *
      * {{{
scala> import com.tresata.spark.datasetops.RichPairDataset
scala> Seq((1, 2), (1, 3)).toDS.reduceByKey(_ + _).show
+-----+---------------------+
|value|ReduceAggregator(int)|
+-----+---------------------+
|    1|                    5|
+-----+---------------------+
      * }}}
      */
    def reduceByKey(f: (V, V) => V)(implicit encK: Encoder[K], encV: Encoder[V]): Dataset[(K, V)] = ds.groupByKey(_._1).mapValues(_._2).reduceGroups(f)

    /** Use a TypedColumn to aggregate the key-value Dataset's values for each key.
      *
      * {{{
scala> import com.tresata.spark.datasetops.RichPairDataset
scala> Seq((1, 2), (1, 3), (2, 4)).toDS.aggByKey(typed.avg(x => x + 2)).show
+-----+-----------------+
|value|TypedAverage(int)|
+-----+-----------------+
|    1|              4.5|
|    2|              6.0|
+-----+-----------------+
      * }}}
      */
    def aggByKey[U1](col1: TypedColumn[V, U1])(implicit encK: Encoder[K], encV: Encoder[V]): Dataset[(K, U1)] =
      ds.groupByKey(_._1).mapValues(_._2).agg(col1)

    /** Use two TypedColumns to aggregate the key-value Dataset's values for each key.
      *
      * {{{
scala> import com.tresata.spark.datasetops.RichPairDataset
scala> Seq((1, 2), (1, 3), (2, 4)).toDS.aggByKey(typed.avg(x => x + 2), typed.sum(x => x)).show
+-----+-----------------+-------------------+
|value|TypedAverage(int)|TypedSumDouble(int)|
+-----+-----------------+-------------------+
|    1|              4.5|                5.0|
|    2|              6.0|                4.0|
+-----+-----------------+-------------------+
      * }}}
      */
    def aggByKey[U1, U2](col1: TypedColumn[V, U1], col2: TypedColumn[V, U2])(implicit encK: Encoder[K], encV: Encoder[V]): Dataset[(K, U1, U2)] =
      ds.groupByKey(_._1).mapValues(_._2).agg(col1, col2)

    /** Use three TypedColumns to aggregate the key-value Dataset's values for each key.
      *
      * {{{
scala> import com.tresata.spark.datasetops.RichPairDataset
scala> Seq((1, 2), (1, 3), (2, 4)).toDS.aggByKey(typed.avg(x => x + 2), typed.sum(x => x), typed.avg(x => x - 2)).show
+-----+-----------------+-------------------+-----------------+
|value|TypedAverage(int)|TypedSumDouble(int)|TypedAverage(int)|
+-----+-----------------+-------------------+-----------------+
|    1|              4.5|                5.0|              0.5|
|    2|              6.0|                4.0|              2.0|
+-----+-----------------+-------------------+-----------------+
      * }}}
      */
    def aggByKey[U1, U2, U3](col1: TypedColumn[V, U1], col2: TypedColumn[V, U2], col3: TypedColumn[V, U3])(implicit encK: Encoder[K], encV: Encoder[V]): Dataset[(K, U1, U2, U3)] =
      ds.groupByKey(_._1).mapValues(_._2).agg(col1, col2, col3)

    /** Use four TypedColumns to aggregate the key-value Dataset's values for each key.
      *
      * {{{
scala> import com.tresata.spark.datasetops.RichPairDataset
scala> val agg = typed.sum((x: Int) => x)
scala> Seq((1, 2), (1, 3), (2, 4)).toDS.aggByKey(agg, agg, agg, agg).show
+-----+-------------------+-------------------+-------------------+-------------------+
|value|TypedSumDouble(int)|TypedSumDouble(int)|TypedSumDouble(int)|TypedSumDouble(int)|
+-----+-------------------+-------------------+-------------------+-------------------+
|    2|                4.0|                4.0|                4.0|                4.0|
|    1|                5.0|                5.0|                5.0|                5.0|
+-----+-------------------+-------------------+-------------------+-------------------+
      * }}}
      */
    def aggByKey[U1, U2, U3, U4](col1: TypedColumn[V, U1], col2: TypedColumn[V, U2], col3: TypedColumn[V, U3], col4: TypedColumn[V, U4])(implicit encK: Encoder[K], encV: Encoder[V]):
        Dataset[(K, U1, U2, U3, U4)] =
      ds.groupByKey(_._1).mapValues(_._2).agg(col1, col2, col3, col4)

    /** Use scala functions to aggregate the key-value Dataset's values for each key: zero, reduce, merge, and finish.
      *
      * {{{
scala> val zero = () => 1.0
scala> val reduce = (x: Double, y: Int) => x / y
scala> val merge = (x: Double, y: Double) => x + y
scala> val finish = (x: Double) => x * 10
scala> import com.tresata.spark.datasetops.RichPairDataset
scala> Seq((1, 2), (1, 3)).toDS.aggByKey(zero, reduce, merge, finish).show
+-----+------------------+
|value|       anon$1(int)|
+-----+------------------+
|    1|18.333333333333332|
+-----+------------------+
      * }}}
      */
    def aggByKey[B, U](zero: () => B, reduce: (B, V) => B, merge: (B, B) => B, finish: B => U)(implicit envK: Encoder[K], envV: Encoder[V], encB: Encoder[B], encU: Encoder[U]): Dataset[(K, U)] = {
      val sqlAgg = aggregatorFromFunctions(zero, reduce, merge, finish)
      ds.groupByKey(_._1).mapValues(_._2).agg(sqlAgg.toColumn)
    }

    /** Use a zero element and scala functions (reduce, merge, and finish) to aggregate the key-value Dataset's values for each key.
      *
      * {{{
scala> val zero = 1.0
scala> val reduce = (x: Double, y: Int) => x / y
scala> val merge = (x: Double, y: Double) => x + y
scala> val finish = (x: Double) => x * 10
scala> import com.tresata.spark.datasetops.RichPairDataset
scala> Seq((1, 2), (1, 3)).toDS.aggByKey(zero, reduce, merge, finish).show
+-----+------------------+
|value|       anon$1(int)|
+-----+------------------+
|    1|18.333333333333332|
+-----+------------------+
      * }}}
      */
    def aggByKey[B, U](zero: B, reduce: (B, V) => B, merge: (B, B) => B, finish: B => U)(implicit envK: Encoder[K], envV: Encoder[V], encB: Encoder[B], encU: Encoder[U]): Dataset[(K, U)] =
      aggByKey(() => zero, reduce, merge, finish)

    /** Count the number rows in the key-value Dataset with each key.
      *
      * {{{
scala> import com.tresata.spark.datasetops.RichPairDataset
scala> Seq((1, 2), (1, 3), (2, 4)).toDS.countByKey.show
+-----+--------+
|value|count(1)|
+-----+--------+
|    1|       2|
|    2|       1|
+-----+--------+
      * }}}
      */
    def countByKey()(implicit encK: Encoder[K]): Dataset[(K, Long)] = ds.groupByKey(_._1).count

    /** Inner join with another key-value Dataset on their keys.
      *
      * {{{
scala> import com.tresata.spark.datasetops.RichPairDataset
scala> Seq((1, 2), (1, 3), (2, 4)).toDS.joinByKey(Seq((1,4)).toDS).show
+---+-----+
| _1|   _2|
+---+-----+
|  1|[2,4]|
|  1|[3,4]|
+---+-----+
      * }}}
      */
    def joinOnKey[V1](other: Dataset[(K, V1)])(implicit encKV: Encoder[(K, V)], encKV1: Encoder[(K, V1)], encKVV1: Encoder[(K, (V, V1))]): Dataset[(K, (V, V1))] = {
      val ds1 = ds.withDefaultFieldNames
      val other1 = other.withDefaultFieldNames
      ds1.joinWith(other1, ds1("_1") === other1("_1"), "inner").map{ x => (x._1._1, (x._1._2, x._2._2)) }
    }

    /** Left outer join with another key-value Dataset on their keys.
      *
      * {{{
scala> import com.tresata.spark.datasetops.RichPairDataset
scala> Seq((1, 2), (1, 3), (2, 4)).toDS.leftOuterJoinByKey(Seq((1,4)).toDS).show
+---+--------+
| _1|      _2|
+---+--------+
|  1|   [2,4]|
|  1|   [3,4]|
|  2|[4,null]|
+---+--------+
      * }}}
      */
    def leftOuterJoinOnKey[V1](other: Dataset[(K, V1)])(implicit encKV: Encoder[(K, V)], encKV1: Encoder[(K, V1)], encKVOptV1: Encoder[(K, (V, Option[V1]))]): Dataset[(K, (V, Option[V1]))] = {
      val ds1 = ds.withDefaultFieldNames
      val other1 = other.withDefaultFieldNames
      ds1.joinWith(other1, ds1("_1") === other1("_1"), "left_outer").map{ x => (x._1._1, (x._1._2, Option(x._2).map(_._2))) }
    }

    /** Right outer join with another key-value Dataset on their keys.
      *
      * {{{
scala> import com.tresata.spark.datasetops.RichPairDataset
scala> Seq((1, 2), (1, 3), (2, 4)).toDS.rightOuterJoinByKey(Seq((1,4)).toDS).show
+---+-----+
| _1|   _2|
+---+-----+
|  1|[3,4]|
|  1|[2,4]|
+---+-----+
      * }}}
      */
    def rightOuterJoinOnKey[V1](other: Dataset[(K, V1)])(implicit encKV: Encoder[(K, V)], encKV1: Encoder[(K, V1)], encKVOptV1: Encoder[(K, (Option[V], V1))]): Dataset[(K, (Option[V], V1))] = {
      val ds1 = ds.withDefaultFieldNames
      val other1 = other.withDefaultFieldNames
      ds1.joinWith(other1, ds1("_1") === other1("_1"), "right_outer").map{ x => (x._2._1, (Option(x._1).map(_._2), x._2._2)) }
    }

    /** Full outer join with another key-value Dataset on their keys.
      *
      * {{{
scala> import com.tresata.spark.datasetops.RichPairDataset
scala> Seq((1, 2), (1, 3), (2, 4)).toDS.fullOuterJoinByKey(Seq((1, 4), (1, 5)).toDS).show
+---+--------+
| _1|      _2|
+---+--------+
|  1|   [2,4]|
|  1|   [2,5]|
|  1|   [3,4]|
|  1|   [3,5]|
|  2|[4,null]|
+---+--------+
      * }}}
      */
    def fullOuterJoinOnKey[V1](other: Dataset[(K, V1)])(implicit encKV: Encoder[(K, V)], encKV1: Encoder[(K, V1)], encKVOptV1: Encoder[(K, (Option[V], Option[V1]))]):
        Dataset[(K, (Option[V], Option[V1]))] = {
      val ds1 = ds.withDefaultFieldNames
      val other1 = other.withDefaultFieldNames
      ds1.joinWith(other1, ds1("_1") === other1("_1"), "full_outer").map{ x => (if (x._1 == null) x._2._1 else x._1._1, (Option(x._1).map(_._2), Option(x._2).map(_._2))) }
    }

    /** Discard the key-value Dataset's values, leaving only the keys.
      *
      * {{{
scala> import com.tresata.spark.datasetops.RichPairDataset
scala> Seq((1, 2), (1, 3), (2, 4)).toDS.keys.show
+-----+
|value|
+-----+
|    1|
|    1|
|    2|
+-----+
      * }}}
      */
    def keys(implicit encK: Encoder[K]): Dataset[K] = ds.map(_._1)

    /** Discard the key-value Dataset's keys, leaving only the values.
      *
      * {{{
scala> import com.tresata.spark.datasetops.RichPairDataset
scala> Seq((1, 2), (1, 3), (2, 4)).toDS.values.show
+-----+
|value|
+-----+
|    2|
|    3|
|    4|
+-----+
      * }}}
      */
    def values(implicit encV: Encoder[V]): Dataset[V] = ds.map(_._2)

    /** Partition the key-value Dataset by key, up to the maximum given number of partitions.
      *
      * {{{
scala> import com.tresata.spark.datasetops.RichPairDataset
scala> val ds = Seq((1, 2), (1, 3), (2, 4)).toDS
scala> ds.rdd.partitions
res1: Array[org.apache.spark.Partition] = Array(org.apache.spark.rdd.ParallelCollectionPartition@20fe, org.apache.spark.rdd.ParallelCollectionPartition@20ff, org.apache.spark.rdd.ParallelCollectionPartition@2100)

scala> ds.partitionByKey(1).rdd.partitions
res1: Array[org.apache.spark.Partition] = Array(org.apache.spark.sql.execution.ShuffledRowRDDPartition@0)
      * }}}
      */
    def partitionByKey(numPartitions: Int)(implicit encKV: Encoder[(K, V)]): Dataset[(K, V)] = {
      val ds1 = ds.withDefaultFieldNames
      ds1.repartition(numPartitions, ds1("_1"))
    }

    /** Partition the key-value Dataset by key.
      *
      * {{{
scala> import com.tresata.spark.datasetops.RichPairDataset
scala> val ds = Seq((1, 2), (1, 3), (2, 4)).toDS
scala> ds.rdd.partitions
res1: Array[org.apache.spark.Partition] = Array(org.apache.spark.rdd.ParallelCollectionPartition@20fe, org.apache.spark.rdd.ParallelCollectionPartition@20ff, org.apache.spark.rdd.ParallelCollectionPartition@2100)

scala> ds.partitionByKey.rdd.partitions
res2: Array[org.apache.spark.Partition] = Array(org.apache.spark.sql.execution.ShuffledRowRDDPartition@0, org.apache.spark.sql.execution.ShuffledRowRDDPartition@1, org.apache.spark.sql.execution.ShuffledRowRDDPartition@2, org.apache.spark.sql.execution.ShuffledRowRDDPartition@3, org.apache.spark.sql.execution.ShuffledRowRDDPartition@4, org.apache.spark.sql.execution.ShuffledRowRDDPartition@5, org.apache.spark.sql.execution.ShuffledRowRDDPartition@6, org.apache.spark.sql.execution.ShuffledRowRDDPartition@7)
      * }}}
      */
    def partitionByKey(implicit encKV: Encoder[(K, V)]): Dataset[(K, V)] = {
      val ds1 = ds.withDefaultFieldNames
      ds1.repartition(ds1("_1"))
    }

    /** Sort the key-value Dataset within partitions by key.
      *
      * {{{
scala> import com.tresata.spark.datasetops.RichPairDataset
scala> val ds = Seq((1, 2), (3, 1), (2, 2), (2, 6), (1, 1)).toDS
scala> ds.partitionByKey(2).rdd.glom.map(_.map(_._1).toSeq).collect
res56: Array[Seq[Int]] = Array(WrappedArray(2, 2), WrappedArray(1, 3, 1))

scala> ds.partitionByKey(2).sortWithinPartitionsByKey.rdd.glom.map(_.map(_._1).toSeq).collect
res57: Array[Seq[Int]] = Array(WrappedArray(2, 2), WrappedArray(1, 1, 3))
      * }}}
      */
    def sortWithinPartitionsByKey(implicit encKV: Encoder[(K, V)]): Dataset[(K, V)] = {
      val ds1 = ds.withDefaultFieldNames
      ds1.sortWithinPartitions(ds1("_1"))
    }
  }
}
