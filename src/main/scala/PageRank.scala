import java.io.File

import org.apache.spark.sql.SparkSession

object PageRank {
  /**
    * 输入：
    * 狄云 [戚芳,0.33333|戚长发,0.333333|卜垣,0.333333]
    * 戚芳 [狄云,0.25 |戚长发,0.25|卜垣,0.5]
    * 戚长发 [狄云,0.33333|戚芳,0.333333|卜垣,0.333333]
    * 卜垣 [狄云,0.25|戚芳,0.5|戚长发,0.25]
    */
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: PageRank <input_path>[ <iter_count>[ output_path]]")
      System.exit(1)
    }

    val outputPath =
      if (args.length > 2) args(2)
      else "/media/steve/SoftwareEngineering/Backups/Documents/专业课程文档/大数据实验/课程设计/output/"
    val spark = SparkSession.builder.appName("Page Rank").getOrCreate()
    val lines = spark.read.textFile(args(0)).rdd
    val iter = if (args.length > 1) args(1).toInt else 10

    // GraphBuilder
    val links = lines.map { line =>
      val splits = line.split("\\s+")
      var link: Map[String, Double] = Map()
      splits(1).substring(1, splits(1).length - 1).split("\\|").foreach { pair =>
        val keyValue = pair.split(",")
        link += (keyValue(0) -> keyValue(1).toDouble)
      }
      (splits(0), link)
    }.cache()
    // 初始PR值
    var ranks = links.mapValues(_ => 1.0)

    // PageRankIter
    for (i <- 1 to iter) {
      val contributes = links.join(ranks).values.flatMap { case (link, rank) =>
        link.keys.map(key => (key, rank * link(key)))
      }
      ranks = contributes.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }

    // PageRank Viewer
    val output = ranks.sortBy(f => -f._2)
    output.saveAsTextFile(outputPath)
    output.collect().foreach(tup => println(s"${tup._1}  ${tup._2} ."))

    spark.stop()
  }
}
