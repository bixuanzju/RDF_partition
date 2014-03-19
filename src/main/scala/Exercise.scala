import org.apache.spark
import org.apache.spark._
import org.apache.spark.SparkContext._

object SimpleExercise {

  def main(args: Array[String]) = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("Exercise")
      .setJars(List("target/scala-2.10/exercise-project_2.10-1.0.jar"))
      .setSparkHome("/Users/jeremybi/spark-0.9.0-incubating-bin-hadoop1")
    val sc = new SparkContext(conf)



    // Divide by predicate
    val file1 = sc.textFile("/Users/jeremybi/Desktop/standard_data/output0_0")
    
    // predicatePair is RDD[(predicate, [(subject, object)])]
    val predicatePair = file1.map(line => line.split("&")).
      map(triple => {
            val regex = ".*#([a-zA-Z]+)>".r
            val predicate = triple(1) match {
              case regex(pred) => pred
              case _ => "noMatch"
            }

            (predicate, (triple(0), triple(2)))
          
          }).groupByKey

    predicatePair.collect.foreach(pair => writeToHDFS(sc, pair, ""))


    // Divide by object in type predicate

    // objectPair is [(subject, object)]
    val objectPair = predicatePair.
      filter(pair => pair._1 == "type").
      first._2.
      map(truple => {
            val regex2 = "<.*#([a-zA-Z]+)>$".r
            val subName =  truple._2 match {
              case regex2(subtype) => subtype
              case _ => "noMatch"
            }

            (subName, truple._1)
          })

    // subjectPair is RDD[(subject, [object])]
    val subjectPair = sc.parallelize(objectPair).groupByKey

    subjectPair.collect.foreach(pair => writeToHDFS(sc, pair, "type/"))

    sc.stop()
  }

  def writeToHDFS(sc: SparkContext, pair: (String, Seq[AnyRef]), path: String) = {
    sc.parallelize(pair._2).
      saveAsTextFile("hdfs://localhost:9000/user/jeremybi/" ++ path ++ pair._1)
  }
  
}
