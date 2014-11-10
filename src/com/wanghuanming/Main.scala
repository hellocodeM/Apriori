package com.wanghuanming

import java.io.PrintWriter

import scala.Array.canBuildFrom
import scala.collection.mutable.Set

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.rdd.RDD

object Main {
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

  def main(args: Array[String]) {
    val sc = new SparkContext("yarn-client", "ming-Apriori")
    val input = sc.textFile(args(0))
    val sumLines = input.count
    val minSupport = (sumLines * 0.85).toInt

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
    var outputPath = if (args(1).endsWith("/")) args(1) else args(1) + "/"
    
    // iteration
    for (i <- 1 to 8) {
      val printer = new PrintWriter(outputPath + "result-" + i)
      cur.collect.foreach(line => {
        var res = new StringBuffer()
        line._1.foreach(x => res.append(x + ","))
        printer.println(res.substring(0, res.length - 1) + ":" + (line._2 * 1.0 / sumLines))
      })
      printer.close()
      val candidate = generateCandidate(totalCandidate, cur.map(_._1).collect)
      totalCandidate ++= candidate
      cur = validateCandidate(transactions, candidate, minSupport)
    }
  }
}
