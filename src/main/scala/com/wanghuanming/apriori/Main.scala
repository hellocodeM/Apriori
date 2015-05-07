package com.wanghuanming.apriori

import java.io.PrintWriter

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkContext}

import scala.Array.canBuildFrom
import scala.collection.immutable.{BitSet => ImBitSet}
import scala.collection.mutable.{BitSet => MuBitSet, Set => MutableSet}


object Main {
  type FIS = ImBitSet

  def main(args: Array[String]) = {
    val sc = new SparkContext()
    val inputPath = args(0)
    val outputPath = if (args(1).endsWith("/")) args(1) else args(1) + "/"
    run(sc, inputPath, outputPath, 0.85)
  }

  def run(sc: SparkContext, inputPath: String, outputPath: String, support: Double) {
    val fs = FileSystem.get(new Configuration())
    val conf = sc.getConf
    val sumCores = conf.getInt("spark.executor.cores", 4) * conf.getInt("spark.executor.instances", 12)
    val partitioner = new HashPartitioner(sumCores * 2)

    val input = sc.textFile(inputPath, conf.getInt("spark.executor.instances", 12))
    val sumLines = input.count
    val minSupport = (sumLines * support).toInt

    val DB = input.map(xs => xs.split(" ").drop(1).map(_.toInt).toSeq -> 1).cache
    val oneFISAndSupport = DB.flatMap { case (fis, support) =>
      fis.map(_ -> support)
    }.reduceByKey(partitioner, _ + _).filter(_._2 >= minSupport)

    val oneFIS = MuBitSet()
    oneFISAndSupport.collect.foreach { case (fis, support) => oneFIS += fis }

    // map sparse item to compact
    val mapTo = oneFIS.zipWithIndex.toMap.withDefaultValue(0)
    val mapFrom = mapTo.map(_.swap)

    val transactions = DB.map { case (xs, support) =>
      val bitset = MuBitSet()
      xs.collect(mapTo).foreach(bitset += _)
      bitset.toImmutable -> support
    }.reduceByKey(partitioner, _ + _)

    var fisSupport = oneFISAndSupport.map { case (fis, support) => ImBitSet(mapTo(fis)) -> support }
    var twoFIS = Set[ImBitSet]()

    for (i <- 1 to 8) {
      saveResults(fisSupport, outputPath + "result-" + i)
      if (i != 8 && fisSupport.count != 0) {
        val tmp = fisSupport.collect
        if (i == 2) twoFIS = tmp.map(_._1).toSet
        val candidate = generateCandidate(twoFIS, tmp.map(_._1))
        fisSupport = computeSupport(transactions, candidate)
      }
    }

    def saveResults(rdd: RDD[(FIS, Int)], path: String) = {
      val writer = new PrintWriter(fs.create(new Path(path)))
      if (rdd.count != 0) {
        rdd.collect.foreach { case (fis, support) =>
          writer.println(fis.map(mapFrom).mkString(",") + ":" + support.toDouble / sumLines)
        }
      }
      writer.close
    }

    def generateCandidate(twoFIS: Set[FIS], previousFIS: Seq[FIS]) = {
      val candidate = MutableSet[FIS]()
      previousFIS.par.foreach { i =>
        val setSize = previousFIS(0).size
        previousFIS.foreach { j =>
          if (i != j) {
            if (setSize == 1)
              candidate.synchronized {
                candidate += (i | j)
              }
            else if ((i & j).size == setSize - 1 && twoFIS.contains(i ^ j))
              candidate.synchronized {
                candidate += (i | j)
              }
          }
        }
      }
      candidate
    }

    def computeSupport(transactions: RDD[(FIS, Int)], candidate: Traversable[FIS]) = {
      val candidateValue = sc.broadcast(candidate)
      if (candidate.size != 0) {
        transactions.flatMap { case (fis, support) =>
          candidateValue.value.collect { case x if x.subsetOf(fis) => x -> support }
        }.reduceByKey(partitioner, _ + _).filter(_._2 >= minSupport)
      } else
        sc.emptyRDD[(FIS, Int)]
    }
  }
}
