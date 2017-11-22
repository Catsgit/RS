package UserCF

import java.io.{File, PrintWriter}

import scala.collection.immutable._
import scala.collection.mutable
import scala.io.Source
class UserCFUtil {
  val test = new mutable.HashMap[Int, Set[Int]]()
  val train = new mutable.HashMap[Int, Set[Int]]()
  val data = new mutable.HashMap[Int, Set[Int]]()
  val w = new mutable.HashMap[Int, mutable.HashMap[Int, Double]]()
  var recallValue: Double = 0.0
  var precisionValue: Double = 0.0
  var coverageValue: Double = 0.0
  var popularityValue: Double = 0.0
  def readData: Unit = {
//    val file = Source.fromFile("/home/hadoop/RS/data/ml-1m/ratings.dat")
    val file = Source.fromFile("/home/hadoop/RS/data/ml-1m/test.dat")
//    val file = Source.fromFile("/home/hadoop/RS/data/test.dat")
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
  def evaluation(num: Int, k: Int): Unit = {
    val recommendItems = scala.collection.mutable.Set[Int]()
    val allItems = scala.collection.mutable.Set[Int]()
    val itemPopularity = mutable.HashMap[Int, Int]()
    var hit: Int = 0
    var all1: Int = 0
    var all2: Int = 0
    var ret: Double = 0
    var n: Int = 0
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
    for((userId, movies) <- train) {
      val rank = getRecommendation(userId, num, k);
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
  def userSimilarity: Unit = {
    //构造物品用户表
    val itemUsers = new mutable.HashMap[Int, mutable.Set[Int]]()
    for((userId, items) <- train) {
      for(item <- items) {
        if(!itemUsers.contains(item)) {
          itemUsers += (item -> mutable.Set[Int]())
        }
        itemUsers(item) += userId
      }
    }
    //建立稀疏矩阵
    val c = new mutable.HashMap[Int, mutable.HashMap[Int, Double]]()
    val n = new mutable.HashMap[Int, Int]()
    for((itemId, users) <- itemUsers) {
      for(userId <- users) {
        if(n.contains(userId)) {
          n(userId) += 1
        } else {
          n += (userId -> 1)
        }
        for(otherUserId <- users) {
          if(userId != otherUserId) {
            if(c.contains(userId)) {
              if(c(userId).contains(otherUserId)) {
//                c(userId)(otherUserId) += 1 / math.log(1 + users.size)
                c(userId)(otherUserId) += 1
              } else {
//                c(userId) += (otherUserId -> 1 / math.log(1 + users.size))
                c(userId) += (otherUserId -> 1)
              }
            } else {
//              c += (userId -> mutable.HashMap[Int, Double](otherUserId -> 1 / math.log((1 + users.size))))
              c += (userId -> mutable.HashMap[Int, Double](otherUserId -> 1))
            }
          }
        }
      }
    }
    //计算用户相似度
    for((userId, otherUsers) <- c) {
      for((otherUserId, cuv) <- otherUsers) {
        if(!w.contains(userId)) {
          w += (userId -> mutable.HashMap[Int, Double](otherUserId -> cuv / math.sqrt(n(userId) * n(otherUserId))))
        } else if(!w(userId).contains(otherUserId)) {
            w(userId) += (otherUserId -> cuv / math.sqrt(n(userId) * n(otherUserId)))
        }
      }
    }
  }
  def getRecommendation(userId: Int, n: Int, k: Int): scala.collection.immutable.ListMap[Int, Double] = {
    val rs = recommend(userId, k)
    val rank = scala.collection.immutable.ListMap(rs.toSeq.sortBy(_._2).reverse:_*).take(n)
    return rank
  }
  def recommend(userId: Int, k: Int): mutable.HashMap[Int, Double] = {
    val rs = mutable.HashMap[Int, Double]()
    val interactedItem = train(userId)
    var userSimilarity = scala.collection.immutable.ListMap(w(userId).toSeq.sortBy(_._2).reverse:_*)
    userSimilarity = userSimilarity.take(k)
    for((otherUserId, wuv) <- userSimilarity) {
      val otherUserItems = train(otherUserId)
      for(item <- otherUserItems) {
        if(!interactedItem.contains(item)) {
          if(!rs.contains(item)) {
            rs += (item -> wuv * 1)
          } else {
            rs(item) += wuv * 1
          }
        }
      }
    }
    return rs
  }
}

object UserCF {
  def main(args: Array[String]): Unit = {
    val d = new UserCFUtil
    d.readData
    d.splitData(8, 1)
    d.userSimilarity
    d.evaluation(10, 5)
    val writer = new PrintWriter(new File("/home/hadoop/UserCF"))
    println("k = 5, 准确率: " + d.precisionValue + " 召回率: " + d.recallValue + " 覆盖率: " + d.coverageValue + " 流行度: " + d.popularityValue)
    writer.println("5" + ":" + d.precisionValue + ":" + d.recallValue + ":" + d.coverageValue + ":" + d.popularityValue)
    d.evaluation(10, 10)
    println("k = 10, 准确率: " + d.precisionValue + " 召回率: " + d.recallValue + " 覆盖率: " + d.coverageValue + " 流行度: " + d.popularityValue)
    writer.println("10" + ":" + d.precisionValue + ":" + d.recallValue + ":" + d.coverageValue + ":" + d.popularityValue)
    d.evaluation(10, 20)
    println("k = 20, 准确率: " + d.precisionValue + " 召回率: " + d.recallValue + " 覆盖率: " + d.coverageValue + " 流行度: " + d.popularityValue)
    writer.println("20" + ":" + d.precisionValue + ":" + d.recallValue + ":" + d.coverageValue + ":" + d.popularityValue)
    d.evaluation(10, 40)
    println("k = 40, 准确率: " + d.precisionValue + " 召回率: " + d.recallValue + " 覆盖率: " + d.coverageValue + " 流行度: " + d.popularityValue)
    writer.println("40" + ":" + d.precisionValue + ":" + d.recallValue + ":" + d.coverageValue + ":" + d.popularityValue)
    d.evaluation(10, 80)
    println("k = 80, 准确率: " + d.precisionValue + " 召回率: " + d.recallValue + " 覆盖率: " + d.coverageValue + " 流行度: " + d.popularityValue)
    writer.println("80" + ":" + d.precisionValue + ":" + d.recallValue + ":" + d.coverageValue + ":" + d.popularityValue)
    d.evaluation(10, 160)
    println("k = 160, 准确率: " + d.precisionValue + " 召回率: " + d.recallValue + " 覆盖率: " + d.coverageValue + " 流行度: " + d.popularityValue)
    writer.println("160" + ":" + d.precisionValue + ":" + d.recallValue + ":" + d.coverageValue + ":" + d.popularityValue)
    writer.close()

  }
}
