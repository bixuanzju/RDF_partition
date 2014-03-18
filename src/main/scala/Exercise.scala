import org.apache.spark
import org.apache.spark._
import org.apache.spark.SparkContext._

object SimpleExercise {

  def main(args: Array[String]) = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("Exercise")
      .setJars(List("target/scala-2.10/exercise-assembly-1.0.jar"))
      .setSparkHome("/Users/jeremybi/spark-0.9.0-incubating-bin-hadoop1")
    val sc = new SparkContext(conf)

    // val graph = Graph.fromEdges(
    // sc.textFile("/Users/jeremybi/graph_data").
    //     map(line => line.split(" ")).
    //     map(componet => Edge(componet(0).toInt, componet(2).toInt, componet(1)))
    //     ,1).cache()

    // val subset1 = graph.subgraph(epred = triplet =>
    //   triplet.attr == "4" && triplet.dstId == 5
    // ).triplets.map(triplet => triplet.srcId)

    // val subset2 = graph.subgraph(epred = triplet =>
    //   triplet.attr == "2" && triplet.dstId == 3
    // ).triplets.map(triplet => triplet.srcId)

    // // Intersection of two RDDs
    // (subset1 subtract (subset1 subtract subset2)).collect.foreach(println)

    val file1 = sc.textFile("/Users/jeremybi/Desktop/standard_data/output0_0")

    file1.map(line => line.split("&")).
      map(triple => (triple(1), (triple(0), triple(2)))).
      groupByKey.collect.foreach(pair => writeToFiles(sc, pair))

    val regex1 = "\\((<.*>),<(.*)>\\)".r
    val regex2 = ".*#([a-zA-Z]+)$".r

    val file2 = sc.textFile("hdfs://localhost:9000/user/jeremybi/type")

    val classTuples = file2.map {
      case regex1(sb, ob) => {
        val subTypeName = ob match {
          case regex2(subtype) => subtype
          case _ => "noMatch"
        }
          (subTypeName, sb)
      }
      case _ => ("noMatch", "noMatch")
    }.groupByKey

    classTuples.collect.foreach(pair => divideByType(sc, pair))

    sc.stop()
  }

  def writeToFiles(sc: SparkContext, pair: (String, Seq[(String, String)])) = {
    val regex = ".*#([a-zA-Z]+)>".r
    val result = pair._1 match {
      case regex(m) => m
      case _ => "noMatch"
    }

    sc.parallelize(pair._2).
      saveAsTextFile("hdfs://localhost:9000/user/jeremybi/" ++ result)
  }

  def divideByType(sc: SparkContext, pair: (String, Seq[String])) {
    sc.parallelize(pair._2).
      saveAsTextFile("hdfs://localhost:9000/user/jeremybi/type/" ++ pair._1)
  }

}
