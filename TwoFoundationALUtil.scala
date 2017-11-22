package TwoFoundationAL

import java.io.{File, PrintWriter}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.breakable
import scala.io.Source

class TwoFoundationALUtil {
  val data = new mutable.HashMap[Int, Set[Int]]()
  val test = new mutable.HashMap[Int, Set[Int]]()
  val train = new mutable.HashMap[Int, Set[Int]]()
  val movieBuffer = ArrayBuffer[Int]()
  var itemPopularity = scala.collection.immutable.ListMap[Int, Int]()

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
          if(!movieBuffer.contains(movieId)) {
            movieBuffer += movieId
          }
          if(train.contains(userId)) {
            train(userId) += movieId
          } else {
            train += (userId -> Set[Int](movieId))
          }
        }
      }
    }
  }
  def randomAlEvaluation(): Unit = {
    val recommendItems = scala.collection.mutable.Set[Int]()
    val allItems = scala.collection.mutable.Set[Int]()
    var hit: Int = 0
    var all1: Int = 0
    var all2: Int = 0
    var ret: Double = 0
    var n: Int = 0
    for((userId, movies) <- train) {
      for(movieId <- movies) {
        allItems += movieId
      }
    }
    for((userId, movies) <- train) {
      val rank = randomAlRecommend(userId);
      for(item <- rank) {
        recommendItems += item
        ret += math.log(1 + itemPopularity(item))
        n += 1
      }
      if(test.contains(userId)) {
        val testMovies = test(userId)
        for(item <- rank) {
          if(testMovies.contains(item)) {
            hit += 1
          }
        }
        all1 += 10
        all2 += testMovies.size
      }
    }
    precisionValue = hit / (all1 * 1.0)
    recallValue = hit / (all2 * 1.0)
    coverageValue = recommendItems.size / (allItems.size * 1.0)
    popularityValue =  ret / (n * 1.0)
  }
  def mostPopularEvaluation(): Unit = {
    val recommendItems = scala.collection.mutable.Set[Int]()
    val allItems = scala.collection.mutable.Set[Int]()
    var hit: Int = 0
    var all1: Int = 0
    var all2: Int = 0
    var ret: Double = 0
    var n: Int = 0
    for((userId, movies) <- train) {
      for(movieId <- movies) {
        allItems += movieId
      }
    }
    for((userId, movies) <- train) {
      val rank = mostPopularRecommend(userId);
      for(item <- rank) {
        recommendItems += item
        ret += math.log(1 + itemPopularity(item))
        n += 1
      }
      if(test.contains(userId)) {
        val testMovies = test(userId)
        for(item <- rank) {
          if(testMovies.contains(item)) {
            hit += 1
          }
        }
        all1 += 10
        all2 += testMovies.size
      }
    }
    precisionValue = hit / (all1 * 1.0)
    recallValue = hit / (all2 * 1.0)
    coverageValue = recommendItems.size / (allItems.size * 1.0)
    popularityValue =  ret / (n * 1.0)
  }
  def randomAlRecommend(userId: Int): mutable.Set[Int] = {
    val rank = mutable.Set[Int]()
    val userMovies = train(userId)
    val moviesLen = movieBuffer.size
    var movieIndex = 0
    for(i <- 0 to 10) {
      println(i)
      movieIndex = scala.util.Random.nextInt(moviesLen)
      while(userMovies.contains(movieBuffer(movieIndex))) {
        movieIndex = scala.util.Random.nextInt(moviesLen)
      }
      rank.add(movieBuffer(movieIndex))
    }
    return rank
  }
  def calItemPopularitu(): Unit = {
    val tempItems = mutable.HashMap[Int, Int]()
    for((userId, movies) <- train) {
      for(movieId <- movies) {
        if(!tempItems.contains(movieId)) {
          tempItems += (movieId -> 1)
        } else {
          tempItems(movieId) += 1
        }
      }
    }
    itemPopularity = scala.collection.immutable.ListMap(tempItems.toSeq.sortBy(_._2).reverse:_*)
  }
  def mostPopularRecommend(userId: Int): mutable.Set[Int] = {
    val rank = mutable.Set[Int]()
    val userMovies = train(userId)
    breakable {
      for(movieId <- itemPopularity.keySet) {
        if(!userMovies.contains(movieId)) {
          rank.add(movieId)
        }
        try {
          if(rank.size == 10) {
            scala.util.control.Breaks.break()
          }
        } catch {
          case e: Exception => e.printStackTrace()
        }
      }
    }
    return rank
  }
}
object RandomAl {
  def main(args: Array[String]): Unit = {
    val t = new TwoFoundationALUtil
    val writer = new PrintWriter(new File("/home/hadoop/TwoFoundationAL"))
    t.readData
    t.splitData(8, 1)
    t.calItemPopularitu()
    t.randomAlEvaluation()
    println("1:" + "准确率: " + t.precisionValue + " 召回率: " + t.recallValue + " 覆盖率: " + t.coverageValue + " 流行度: " + t.popularityValue)
    writer.println("1" + ":" + t.precisionValue + ":" + t.recallValue + ":" + t.coverageValue + ":" + t.popularityValue)
    t.mostPopularEvaluation()
    println("2:" + "准确率: " + t.precisionValue + " 召回率: " + t.recallValue + " 覆盖率: " + t.coverageValue + " 流行度: " + t.popularityValue)
    writer.println("2" + ":" + t.precisionValue + ":" + t.recallValue + ":" + t.coverageValue + ":" + t.popularityValue)
    writer.close()
  }
}
