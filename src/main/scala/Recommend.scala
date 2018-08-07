import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD
import scala.io.Source
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import scala.collection.immutable.Map

/**
  * Created by 62362 on 2018/8/7.
  */
object Recommend {
  
  def recommend(model: MatrixFactorizationModel, movieTile: Map[Int, String]) = {
    var choose = ""
    while (choose != "3") {
      print("请选择要推荐类型 1.针对用户推荐电影 2.针对电影推荐给感兴趣的用户 3.离开")
      choose = readLine()
      if (choose == "1") {
        print("请输入用户id：")
        val inputUserID = readLine()
        RecommendMovies(model, movieTile, inputUserID.toInt)
      } else if (choose == "2") {
        print("请输入电影id：")
        val inputMovieID = readLine()
        RecommendUsers(model, movieTile, inputMovieID.toInt)
      }
    }
  }

  def RecommendMovies(model: MatrixFactorizationModel, movieTile: Map[Int, String], inputUserID: Int) = {
    val RecommendMovie = model.recommendProducts(inputUserID, 10)
    val i = 1
    println("针对用户id" + inputUserID + "推荐下列电影：")
    RecommendMovie.foreach {
      r => println(i.toString + "." + movieTile(r.product) + "评分：" + r.rating.toString())
        i + 1
    }
  }

  def RecommendUsers(model: MatrixFactorizationModel, movieTile: Map[Int, String], inputMovieID: Int) = {
    val RecommendUser = model.recommendUsers(inputMovieID, 10)
    var i = 1
    println("针对电影id" + inputMovieID + "推荐下列用户id：")
    RecommendUser.foreach {
      r => println(i.toString + "用户id：" + r.user + " 评分：" + r.rating)
        i = i + 1
    }
  }

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def PrepareData(): (RDD[Rating], Map[Int, String]) = {
    val sc = new SparkContext(new SparkConf().setAppName("Recommend").setMaster("10.16.0.132"))
    print("开始读取用户评分数据中...")
    val rawUserData = sc.textFile("resources\\data\\u.data")
    val rawRatings = rawUserData.map(_.split("\t").take(3))
    val ratingsRDD = rawRatings.map {
      case Array(user, movie, rating) => Rating(user.toInt, movie.toInt, rating.toDouble)
    }
    println("共计：" + ratingsRDD.count().toString() + "条 ratings")

    print("开始读取电影数据中...")
    val itemRDD = sc.textFile("resources\\data\\u.item")
    val movieTile = itemRDD.map(line => line.split("\\|").take(2)).map(array => (array(0).toInt, array(1))).collect().toMap

    val numRatings = ratingsRDD.count()
    val numUsers = ratingsRDD.map(_.user).distinct().count()
    val numMovies = ratingsRDD.map(_.product).distinct().count()
    println("共计：ratings：" + numRatings + "User " + numUsers + "Movie " + numMovies)
    return (ratingsRDD, movieTile)
  }


}
