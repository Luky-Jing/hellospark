import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by 62362 on 2018/8/4.
  */
object RunWordCount {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    println("开始运行RunWordCount")
    val sc = new SparkContext(new SparkConf().setAppName("wordcount"))
    println("开始读取文本文件")
    val textFile = sc.textFile("hdfs://xdata-lpj-hadoop-master.sre.name:9000/lipeijing/hadoop-env.sh")
    println("开始创建RDD..")
    val countsRDD = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_+_)
    println("开始保存到文本文件..")
    try {
      countsRDD.saveAsTextFile("hdfs://xdata-lpj-hadoop-master.sre.name:9000/lipeijing/hadoop-env.sh.out")
      println("保存成功")
    } catch {
      case e: Exception => println("输出目录已经存在，请先删除原有目录");
    }
  }

}
