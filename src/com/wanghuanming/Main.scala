package com.wanghuanming

import java.io.{BufferedOutputStream, PrintWriter}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import scala.Array.canBuildFrom
import scala.collection.mutable.Set

object Main {

  def main(args: Array[String]) {
    val sc = new SparkContext()
    val input = sc.textFile(args(0))
    val sumLines = input.count
    val minSupport = (sumLines * 0.85).toInt
    val outputPath = if (args(1).endsWith("/")) args(1) else args(1) + "/"
    val fs = FileSystem.get(new Configuration()) //HDFS

    // pre process
    val DB = input.map(line => (line.split(" ").drop(1).toList, 1)).reduceByKey(_ + _).cache
    val oneFIS = DB.flatMap(line => line._1.map((_, line._2)))
      .reduceByKey(_ + _).filter(_._2 >= minSupport).collect
    // compress
    val transactions = DB.map(line => {
      (line._1.filter(item => oneFIS.exists(_._1 == item)), line._2)
    }).reduceByKey(_ + _)
    var cur = sc.parallelize(oneFIS.map(item => (Set(item._1), item._2)))
    var totalCandidate = Set[Set[String]]()
    // iteration
    for (i <- 2 to 8) {
      val writer = new PrintWriter(
        new BufferedOutputStream(fs.create(new Path(outputPath + "result-" + i)), 256))
      val tmp = cur.collect
      tmp.foreach(line => {
        var output = new StringBuffer()
        line._1.foreach(x => output.append(x + ","))
        writer.println(output.substring(0, output.length - 1) + ":" + (line._2 * 1.0 / sumLines))
      })
      writer.close()
      val candidate = generateCandidate(totalCandidate, tmp.map(_._1))
      totalCandidate ++= candidate
      cur = validateCandidate(transactions, candidate, minSupport)
    }
    sc.stop

    def generateCandidate(totalCandidate: Set[Set[String]], keys: Array[Set[String]]) = {
      var candidate = Set[Set[String]]()
      for (i <- keys)
        for (j <- keys) {
          if (i != j && keys.size != 0) {
            // one FIS, combine them directly
            if (keys(0).size == 1)
              candidate += i ++ j
            else if ((i != j) && (i & j).size == (i.size - 1) && totalCandidate.contains(i ++ j -- (i & j)))
              candidate += i ++ j
          }
        }
      candidate
    }

    def validateCandidate(transactions: RDD[(List[String], Int)], candidate: Set[Set[String]], minSupport: Int) = {
      transactions.flatMap(line => {
        var tmp = Set[(Set[String], Int)]()
        for (can <- candidate) {
          if (!can.exists(item => !line._1.contains(item)))
            tmp += can -> line._2
        }
        tmp
      }).reduceByKey(_ + _).filter(_._2 >= minSupport)
    }
  }

}
