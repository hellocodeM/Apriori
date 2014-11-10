package com.wanghuanming

import java.io.PrintWriter

import scala.Array.canBuildFrom
import scala.annotation.migration
import scala.collection.mutable.Set

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.rdd.RDD

object Main {
  def generateKeys(FIS: RDD[(Set[String], Int)]): Array[Set[String]] = {
    FIS.map(_._1).collect()
  }

  def generateCandidate(totalCandidate: Set[Set[String]], keys: Array[Set[String]]) = {
    var candidate = Set[Set[String]]()
    if (keys.size != 0 && keys(0).size == 1) {
      for (i <- keys)
        for (j <- keys)
          if (i != j)
            candidate += i ++ j
    } else {
      for (i <- keys) {
        for (j <- keys) {
          if ((i != j) && (i & j).size == (i.size - 1) && totalCandidate.contains(i ++ j -- (i & j))) {
            candidate += (i ++ j)
          }
        }
      }
    }
    candidate
  }

  def validateCandidate(transactions: RDD[(Array[String], Int)], candidate: Set[Set[String]], limit: Int) = {
    transactions.flatMap(line => {
      var tmp = Set[(Set[String], Int)]()
      for (can <- candidate) {
        if (!can.exists(item => !line._1.contains(item)))
          tmp += can -> line._2
      }
      tmp
    }).reduceByKey(_ + _).filter(_._2 > limit)
  }

  def main(args: Array[String]) {
    val sc = new SparkContext("yarn-client", "ming-Apriori")
    val input = sc.textFile(args(0))
    val sum = input.count()
    val limit = (sum * 0.85).toInt

    // pre process
    var DB = input.map(line => line.split(" ").drop(1)).cache
    var oneFIS = DB.flatMap(items => items.map((_, 1))).reduceByKey(_ + _).filter(_._2 >= limit).collect.toList
    // compress
    var transactions = DB.map(line => {
      (line.filter(item => oneFIS.exists(_._1 == item)), 1)
    }).reduceByKey(_ + _)
    var cur = sc.parallelize(oneFIS.map(item => (Set(item._1), item._2)))
    var totalCandidate = Set[Set[String]]()

    // save as file
    var outputPath = args(1)
    if (! outputPath.endsWith("/"))
      outputPath += "/"
    val writer = new PrintWriter(outputPath + "result-1")
    cur.collect.foreach(line => {
      var res = new StringBuffer()
      line._1.foreach(x => res.append(x + ","))
      writer.println(res.substring(0, res.length - 1) + ":" + (line._2 * 1.0 / sum))
    })
    writer.close()

    // iteration
    for (i <- 1 to 7) {
      val candidate = generateCandidate(totalCandidate, generateKeys(cur))
      totalCandidate ++= candidate
      cur = validateCandidate(transactions, candidate, limit)
      val printer = new PrintWriter(outputPath + "result-" + (i + 1))
      cur.collect.foreach(line => {
        var res = new StringBuffer()
        line._1.foreach(x => res.append(x + ","))
        printer.println(res.substring(0, res.length - 1) + ":" + (line._2 * 1.0 / sum))
      })
      printer.close()
    }
  }
}
