package ItemSimilarity

import java.io.{File, PrintWriter}

import scala.collection.immutable.ListMap
import scala.collection.mutable
import scala.io.Source
import scala.util.control.Breaks._

class ItemSimilarity {
  val data = mutable.HashMap[Int, Set[Int]]()
  val movieInfo = mutable.HashMap[Int, String]()
  val w = mutable.HashMap[Int, scala.collection.immutable.ListMap[Int, Double]]()
  var result = ListMap[Int, Double]()

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

  def readMovieInfo: Unit = {
    val file = Source.fromFile("/home/hadoop/RS/data/ml-1m/newmovies.dat")
    for(line <- file.getLines()) {
      val movieId = line.split("::")(0).toInt
      val movieName = line.split("::")(1)
      movieInfo += (movieId -> movieName)
    }
  }

  def itemSimilarity: Unit = {
    val userItems = mutable.HashMap[Int, Set[Int]]()
    for((userId, movies) <- data) {
      for(movieId <- movies) {
        if(userItems.contains(userId)) {
          userItems(userId) += movieId
        } else {
          userItems += (userId -> Set[Int](movieId))
        }
      }
    }
    val n = mutable.HashMap[Int, Int]()
    val c = mutable.HashMap[Int, mutable.HashMap[Int, Int]]()
    for((userId, movies) <- data) {
      for(movieId <- movies) {
        if(n.contains(movieId)) {
          n(movieId) += 1
        } else {
          n += (movieId -> 1)
        }
        for(otherMovieId <- movies) {
          if(otherMovieId != movieId) {
            if(c.contains(movieId)) {
              if (c(movieId).contains(otherMovieId)) {
                c(movieId)(otherMovieId) += 1
              } else {
                c(movieId) += (otherMovieId -> 1)
              }
            } else {
              c += (movieId -> mutable.HashMap[Int, Int](otherMovieId -> 1))
            }
          }
        }
      }
    }
    for((movieId, otherMovies) <- c) {
      for((otherMovieId, cuv) <- otherMovies) {
        if(w.contains(movieId)) {
          w(movieId) += (otherMovieId -> cuv / math.sqrt(n(movieId) * n(otherMovieId)))
        } else {
          w += (movieId -> scala.collection.immutable.ListMap[Int, Double](otherMovieId -> cuv / math.sqrt(n(movieId) * n(otherMovieId))))
        }
      }
      w(movieId) = ListMap(w(movieId).toSeq.sortBy(_._2).reverse:_*)
    }
  }
}

object ItemSimilarity {
  def main(args: Array[String]): Unit = {
    val i = new ItemSimilarity;
    val writer = new PrintWriter(new File("/home/hadoop/RS/result/ItemCF/ItemSimilarity"))
    i.readData
    i.readMovieInfo
    i.itemSimilarity
    var n = 0
    val l = List[Int](588, 2924, 1, 2762, 2571, 356)
    for(movieId <- l) {
      n = 0
      breakable {
        for((otherMovieId, wuv) <- i.w(movieId)) {
          println(i.movieInfo(movieId) + "\t" + i.movieInfo(otherMovieId) + "\t" + wuv)
          writer.println(i.movieInfo(movieId) + "\t" + i.movieInfo(otherMovieId) + "\t" + wuv)
          n += 1
          if(n == 5) {
            break()
            writer.println()
          }
        }
      }
    }
    writer.close()
  }
}
