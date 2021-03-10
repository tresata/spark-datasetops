package com.tresata.spark.datasetops

import org.scalatest.funspec.AnyFunSpec
import org.apache.spark.sql.expressions.scalalang.typed

class RichPairDatasetSpec extends AnyFunSpec with SparkSuite {
  import spark.implicits._

  describe("RichPairDataset") {
    it("should mapValues") {
      assert(Seq((1, 2), (1, 3), (2, 4)).toDS.mapValues(_ + 1) === Seq((1, 3), (1, 4), (2, 5)))
    }

    it("should reduceByKey") {
      assert(Seq((1, 2), (1, 3), (2, 4)).toDS.reduceByKey(_ + _) === Seq((1, 5), (2, 4)))
    }

    it("should aggByKey") {
      assert(Seq((1, 2), (1, 3), (2, 4)).toDS.aggByKey(typed.avg(x => x), typed.count(x => x)) === Seq((1, 2.5, 2L), (2, 4.0, 1L)))
    }

    it("should aggByKey using functions") {
      assert(Seq((1, 2), (1, 3), (2, 4)).toDS.aggByKey(0, { (b: Int, a: Int) => b + a }, { (b1: Int, b2: Int) => b1 + b2 }, { (b: Int) => b }) === Seq((1, 5), (2, 4)))
    }

    it("should countByKey") {
      assert(Seq((1, 2), (1, 3), (2, 4)).toDS.countByKey === Seq((1, 2), (2, 1)))
    }

    it("should joinOnKey") {
      assert(Seq((1, 2), (1, 3), (2, 4)).toDS.joinOnKey(Seq((1, "a"), (1, "b"), (2, "c")).toDS) ===
        Seq((1, (2, "a")), (1, (2, "b")), (1, (3, "a")), (1, (3, "b")), (2, (4, "c"))))
    }

    it("should leftOuterJoinOnKey") {
      assert(Seq((1, 2), (1, 3), (2, 4)).toDS.leftOuterJoinOnKey(Seq((1, "a"), (3, "b")).toDS) ===
        Seq((1, (2, Some("a"))), (1, (3, Some("a"))), (2, (4, None))))
    }

    it("should rightOuterJoinOnKey") {
      assert(Seq((1, 2), (1, 3), (2, 4)).toDS.rightOuterJoinOnKey(Seq((1, "a"), (3, "b")).toDS) ===
        Seq((1, (Some(2), "a")), (1, (Some(3), "a")), (3, (None, "b"))))
    }

    it("should fullOuterJoinOnKey") {
      assert(Seq((1, 2), (1, 3), (2, 4)).toDS.fullOuterJoinOnKey(Seq((1, "a"), (3, "b")).toDS) ===
        Seq((1, (Some(2), Some("a"))), (1, (Some(3), Some("a"))), (2, (Some(4), None)), (3, (None, Some("b")))))
    }

    it("should extract keys") {
      assert(Seq((1, 2), (1, 3), (2, 4)).toDS.keys === Seq(1, 1, 2))
    }

    it("should extract values") {
      assert(Seq((1, 2), (1, 3), (2, 4)).toDS.values === Seq(2, 3, 4))
    }

    it("should partition by key") {
      val setOfKeySets = Seq((1, 2), (1, 3), (2, 4), (1, 5), (3, 1), (2, 2), (2, 6), (1, 1)).toDS.partitionByKey(2)
        .rdd.glom.map(_.map(_._1).toSet).collect.toSet
      assert(setOfKeySets.map(_.size).sum === setOfKeySets.flatten.size)
    }

    it("should sort by key within partitions") {
      val seqOfKeySeqs = Seq((1, 2), (1, 3), (2, 4), (1, 5), (3, 1), (2, 2), (2, 6), (1, 1)).toDS.partitionByKey(2).sortWithinPartitionsByKey
        .rdd.glom.map(_.map(_._1).toSeq).collect.toSeq
      assert(seqOfKeySeqs.forall(keySeq => keySeq === keySeq.sorted))
    }
  }
}
