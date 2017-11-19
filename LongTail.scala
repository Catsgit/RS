import java.io.{File, PrintWriter}

import scala.collection.mutable
import scala.io.Source

class LongTailUtil {
  val data = mutable.HashMap[Int, Set[Int]]()   //数据集
  var ipNum = scala.collection.immutable.ListMap[Int, Int]()  //电影流行度k-流行度为k的电影总数
  var upNum = scala.collection.immutable.ListMap[Int, Int]()  //用户活跃度k-活跃度为k的用户总数
  var userAct = scala.collection.immutable.ListMap[Int, Int]() //用户-用户感兴趣的电影数
  val itemPop = mutable.HashMap[Int, Int]() //电影-对该电影感兴趣的人数
  var upip = scala.collection.immutable.ListMap[Int, Double]()
  def readData: Unit = {
    val file = Source.fromFile("/home/hadoop/RS/data/ml-1m/ratings.dat")
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
  //计算物品流行度k 以及流行度为k的物品数目
  def itemPopularity: Unit = {
    val ip1 = mutable.HashMap[Int, Int]() //电影的流行度(人数)-流行度为K的物品的总数
    for((userId, movies) <- data) {
      for(movieId <- movies) {
        if(itemPop.contains(movieId)) {
          itemPop(movieId) += 1
        } else {
          itemPop += (movieId -> 1)
        }
      }
    }
    for((userId, itemP) <- itemPop) {
      if(ip1.contains(itemP)) {
        ip1(itemP) += 1
      } else {
        ip1 += (itemP -> 1)
      }
    }
    ipNum = scala.collection.immutable.ListMap(ip1.toSeq.sortBy(_._1):_*) //最终结果 排序
  }
  //计算用户活跃度k 以及活跃度为k的用户数目
  def userActivity: Unit = {
    val up = mutable.HashMap[Int, Int]()
    val up1 = mutable.HashMap[Int, Int]()
    for((userId, movies) <- data) {
      up += (userId -> movies.size)
    }
    userAct = scala.collection.immutable.ListMap(up.toSeq.sortBy(_._2):_*)
    for((userId, userVitality) <- up) {
      if(up1.contains(userVitality)) {
        up1(userVitality) += 1
      } else {
        up1 += (userVitality -> 1)
      }
    }
    upNum = scala.collection.immutable.ListMap(up1.toSeq.sortBy(_._1):_*) //最终结果 进行排序
  }
  //计算用户活跃度以及对应的平均物品流行度
  def userActivityAndItemPopularity: Unit = {
    var sum = 0.0
    var num = 0
    var flag = false
    var previousUA = 0
    val uip = mutable.HashMap[Int, Double]()
    for((userId, userVitality) <- userAct) {
      if(flag == false) {
        previousUA = userVitality
        flag = true
      }
      for(movieId <- data(userId)) {
        if(userVitality != previousUA) {
          uip += (previousUA -> sum / num)
          previousUA = userVitality
          num = 0
          sum = 0.0
        }
        num += 1
        sum += itemPop(movieId)
      }
    }
    upip = scala.collection.immutable.ListMap(uip.toSeq.sortBy(_._1):_*)  //最终结果 排序
  }
  //将结果输出到文件
  def writeToFile: Unit = {
    var writer = new PrintWriter(new File("/home/hadoop/RS/itemPopularity"))
    for((itemP, num) <- ipNum) {
      writer.println(itemP + " " + num)
    }
    writer.close()
    writer = new PrintWriter(new File("/home/hadoop/RS/userVitality"))
    for((userV, num) <- upNum) {
      writer.println(userV + " " + num)
    }
    writer.close()
    writer = new PrintWriter(new File("/home/hadoop/RS/userVitalityAndItemPopularity"))
    for((userV, itemP) <- upip) {
      writer.println(userV + " " + itemP)
    }
    writer.close()
  }
}

object LongTail {
  def main(args: Array[String]): Unit = {
    val l = new LongTailUtil()
    l.readData
    l.itemPopularity
    l.userActivity
    l.userActivityAndItemPopularity
    l.writeToFile
  }
}
