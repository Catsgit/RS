package ItemCFIUF

import java.io.{File, PrintWriter}

import scala.collection.mutable
import scala.io.Source

class ItemCFCompareUtil {
  val test = mutable.HashMap[Int, Set[Int]]()
  val train = mutable.HashMap[Int, Set[Int]]()
  val data = mutable.HashMap[Int, Set[Int]]()
  val w= mutable.HashMap[Int, scala.collection.immutable.ListMap[Int, Double]]()

  val allItems = scala.collection.mutable.Set[Int]()
  val itemPopularity = mutable.HashMap[Int, Int]()

  var recallValue: Double = 0.0
  var precisionValue: Double = 0.0
  var coverageValue: Double = 0.0
  var popularityValue: Double = 0.0

  def readData: Unit = {
    val file = Source.fromFile("/home/hadoop/RS/data/ml-1m/test.dat")
    for(line <- file.getLines()) {
      val userId = line.split("::")(0).toInt
      val movieId = line.split("::")(1).toInt
      if(data.contains(userId)) {
        data(userId) += movieId
      } else {
        data += (userId -> Set[Int](movieId))
      }
    }
  }

  def splitData(m: Int, k: Int): Unit = {
    var trainNum = 0
    var testNum = 0
    val r = scala.util.Random
    r.setSeed(k)
    for((userId, movies) <- data) {
      for(movieId <- movies) {
        if(r.nextInt(m) == k) {
          if(test.contains(userId)) {
            test(userId) += movieId
          } else {
            test += (userId -> Set[Int](movieId))
          }
        } else {
          if(train.contains(userId)) {
            train(userId) += movieId
          } else {
            train += (userId -> Set[Int](movieId))
          }
        }
      }
    }
  }

  def calItemPopularity: Unit = {
    for((userId, movies) <- train) {
      for(movieId <- movies) {
        if(!itemPopularity.contains(movieId)) {
          itemPopularity += (movieId -> 1)
        } else {
          itemPopularity(movieId) += 1
        }
        allItems += movieId
      }
    }
  }

  //itemCF
  def itemCFEvaluation(num: Int, k: Int): Unit = {
    val recommendItems = scala.collection.mutable.Set[Int]()
    var hit: Int = 0
    var all1: Int = 0
    var all2: Int = 0
    var ret: Double = 0
    var n: Int = 0
    for((userId, movies) <- train) {
      val rank = getItemCFRecommendation(userId, num, k);
      for(item <- rank.keySet) {
        recommendItems += item
        ret += math.log(1 + itemPopularity(item))
        n += 1
      }
      if(test.contains(userId)) {
        val testMovies = test(userId)
        for(item <- rank.keySet) {
          if(testMovies.contains(item)) {
            hit += 1
          }
        }
        all1 += num
        all2 += testMovies.size
      }
    }
    precisionValue = hit / (all1 * 1.0)
    recallValue = hit / (all2 * 1.0)
    coverageValue = recommendItems.size / (allItems.size * 1.0)
    popularityValue =  ret / (n * 1.0)
  }

  def getItemCFRecommendation(userId: Int, n: Int, k: Int): scala.collection.immutable.ListMap[Int, Double] = {
   val rs = itemCFRecommend(userId, k)
   val rank = scala.collection.immutable.ListMap(rs.toSeq.sortBy(_._2).reverse:_*).take(n)
   return rank
  }

  def itemCFRecommend(userId: Int, k: Int): mutable.HashMap[Int, Double] = {
    val rs = mutable.HashMap[Int, Double]()
    val ru = train(userId)
    for(movieId <- ru) {
      val itemSim = w(movieId).take(k)
      for((otherMovieId, wuv) <-itemSim) {
        if(!ru.contains(otherMovieId)) {
          if(rs.contains(otherMovieId)) {
            rs(otherMovieId) += 1 * wuv
          } else {
            rs += (otherMovieId -> 1 * wuv)
          }
        }
      }
    }
    return rs
  }

  def itemCFSimilarity(): Unit = {
    w.clear()
    val n = mutable.HashMap[Int, Int]()
    val c = mutable.HashMap[Int, mutable.HashMap[Int, Double]]()
    for((userId, movies) <- train) {
      for(movieId <- movies) {
        if(n.contains(movieId)) {
          n(movieId) += 1
        } else {
          n += (movieId -> 1)
        }
        for(otherMovieId <- movies) {
          if(otherMovieId != movieId) {
            if(c.contains(movieId)) {
              if(c(movieId).contains(otherMovieId)) {
                c(movieId)(otherMovieId) += 1
              } else {
                c(movieId) += (otherMovieId -> 1)
              }
            } else {
              c += (movieId -> mutable.HashMap(otherMovieId -> 1))
            }
          }
        }
      }
    }
    for((movieId, otherMovies) <- c) {
      for((otherMovieId, cuv) <- otherMovies) {
        if(w.contains(movieId)) {
          w(movieId) += (otherMovieId -> c(movieId)(otherMovieId) / math.sqrt(n(movieId) * n(otherMovieId)))
        } else {
          w += (movieId -> scala.collection.immutable.ListMap(otherMovieId -> c(movieId)(otherMovieId) / math.sqrt(n(movieId) * n(otherMovieId))))
        }
      }
      w(movieId) = scala.collection.immutable.ListMap(w(movieId).toSeq.sortBy(_._2).reverse:_*)
    }
  }

  //ItemCFIUF
  def itemCFIUFEvaluation(num: Int, k: Int): Unit = {
    val recommendItems = scala.collection.mutable.Set[Int]()
    var hit: Int = 0
    var all1: Int = 0
    var all2: Int = 0
    var ret: Double = 0
    var n: Int = 0
    for((userId, movies) <- train) {
      val rank = getItemCFIUFRecommendation(userId, num, k);
      for(item <- rank.keySet) {
        recommendItems += item
        ret += math.log(1 + itemPopularity(item))
        n += 1
      }
      if(test.contains(userId)) {
        val testMovies = test(userId)
        for(item <- rank.keySet) {
          if(testMovies.contains(item)) {
            hit += 1
          }
        }
        all1 += num
        all2 += testMovies.size
      }
    }
    precisionValue = hit / (all1 * 1.0)
    recallValue = hit / (all2 * 1.0)
    coverageValue = recommendItems.size / (allItems.size * 1.0)
    popularityValue =  ret / (n * 1.0)
  }



  def getItemCFIUFRecommendation(userId: Int, n: Int, k: Int): scala.collection.immutable.ListMap[Int, Double] = {
    val rs = itemCFIUFRecommend(userId, k)
    val rank = scala.collection.immutable.ListMap(rs.toSeq.sortBy(_._2).reverse:_*).take(n)
    return rank
  }



  def itemCFIUFRecommend(userId: Int, k: Int): mutable.HashMap[Int, Double] = {
    val rs = mutable.HashMap[Int, Double]()
    val ru = train(userId)
    for(movieId <- ru) {
      val itemSim = w(movieId).take(k)
      for((otherMovieId, wuv) <-itemSim) {
        if(!ru.contains(otherMovieId)) {
          if(rs.contains(otherMovieId)) {
            rs(otherMovieId) += 1 * itemSim(otherMovieId)
          } else {
            rs += (otherMovieId -> 1 * itemSim(otherMovieId))
          }
        }
      }
    }
    return rs
  }

  def itemCFIUFSimilarity(): Unit = {
    w.clear()
    val n = mutable.HashMap[Int, Int]()
    val c = mutable.HashMap[Int, mutable.HashMap[Int, Double]]()
    for((userId, movies) <- train) {
      for(movieId <- movies) {
        if(n.contains(movieId)) {
          n(movieId) += 1
        } else {
          n += (movieId -> 1)
        }
        for(otherMovieId <- movies) {
          if(otherMovieId != movieId) {
            if(c.contains(movieId)) {
              if(c(movieId).contains(otherMovieId)) {
                c(movieId)(otherMovieId) += 1 / math.log(1 + movies.size * 1.0)
              } else {
                c(movieId) += (otherMovieId -> 1 / math.log(1 + movies.size * 1.0))
              }
            } else {
              c += (movieId -> mutable.HashMap(otherMovieId -> 1 / math.log(1 + movies.size * 1.0)))
            }
          }
        }
      }
    }
    println("c finished")
    var x = 1
    for((movieId, otherMovies) <- c) {
      for((otherMovieId, cuv) <- otherMovies) {
        if(w.contains(movieId)) {
          w(movieId) += (otherMovieId -> c(movieId)(otherMovieId) / math.sqrt(n(movieId) * n(otherMovieId)))
        } else {
          w += (movieId -> scala.collection.immutable.ListMap(otherMovieId -> c(movieId)(otherMovieId) / math.sqrt(n(movieId) * n(otherMovieId))))
        }
      }
      w(movieId) = scala.collection.immutable.ListMap(w(movieId).toSeq.sortBy(_._2).reverse:_*)
      println(x)
      x += 1
    }
    println("w finished")
  }

}

object ItemCFIUF {
  def main(args: Array[String]): Unit = {
    val i = new ItemCFCompareUtil
    val writer = new PrintWriter(new File("/home/hadoop/ItemCFCompare"))
    i.readData
    i.splitData(8, 1)
    i.calItemPopularity
    i.itemCFSimilarity()
    i.itemCFEvaluation(10, 10)
    println("ItemCF 准确率: " + i.precisionValue + " 召回率: " + i.recallValue + " 覆盖率: " + i.coverageValue + " 流行度: " + i.popularityValue)
    writer.println(i.precisionValue + ":" + i.recallValue + ":" + i.coverageValue + ":" + i.popularityValue)
    i.itemCFIUFSimilarity()
    i.itemCFIUFEvaluation(10, 10)
    println("ItemCFIUF 准确率: " + i.precisionValue + " 召回率: " + i.recallValue + " 覆盖率: " + i.coverageValue + " 流行度: " + i.popularityValue)
    writer.println(i.precisionValue + ":" + i.recallValue + ":" + i.coverageValue + ":" + i.popularityValue)
    writer.close()
  }
}
