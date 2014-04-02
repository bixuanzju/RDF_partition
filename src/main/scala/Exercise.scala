import org.apache.spark
import org.apache.spark._
import org.apache.spark.SparkContext._

object SimpleExercise {

  def main(args: Array[String]) = {

    // Initialization
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("Exercise")
      .setJars(List("target/scala-2.10/exercise-project_2.10-1.0.jar"))
      .setSparkHome("/Users/jeremybi/spark-0.9.0-incubating-bin-hadoop1")
    val sc = new SparkContext(conf)
    val filePath = "/Users/jeremybi/Desktop/standard_data/output0_0"


    // Step 1
    // Divide by predicate
    // predicatePair is RDD[(predicate, [(subject, object)])]
    val predicatePair = sc.textFile(filePath).
      map(line => line.split("&")).
      map(triple => {
            val Regex = ".*#([a-zA-Z]+)>".r
            val predicate = triple(1) match {
              case Regex(pred) => pred
              case _ => "noMatch"
            }

            (predicate, (triple(0), triple(2)))

          }).groupByKey.cache


    // Step 2
    // Subdivide by object in type predicate
    // classPair is Map[class, Seq(class, object)]
    val classPair = predicatePair.
      filter(pair => pair._1 == "type").
      first._2.
      map(truple => {
            val Regex2 = "<.*#([a-zA-Z]+)>$".r
            val objName =  truple._2 match {
              case Regex2(subtype) => subtype
              case _ => "noMatch"
            }

            (objName, truple._1)
          }).groupBy(_._1)

    classPair.foreach(pair => writeToHDFS(sc, pair._2, "type/" ++ pair._1))


    // Step 3
    // Subdivide all predicates other than type predicate

    // by object
    val split1 = predicatePair.filter(pair => pair._1 != "type").
      map(pair =>
        (pair._1, pair._2.
           map(tuple => (findClass(tuple._2, classPair), tuple)).
           groupBy(_._1)))

    split1.collect.
      foreach(pair =>   // (predicate, Map[class, Seq[(class, (s,o))]])
        pair._2.foreach(tuple => // (class, Seq[(class, (s,o))])
          writeToHDFS(sc, tuple._2, pair._1 ++ "/" ++ pair._1 ++ "_" ++ tuple._1)
        )
      )

    // by subject
    val split2 = predicatePair.filter(pair => pair._1 != "type").
      map(pair =>
        (pair._1, pair._2.
           map(tuple => (findClass(tuple._1, classPair), tuple)).
           groupBy(_._1)))

    split2.collect.
      foreach(pair =>   // (predicate, Map[class, Seq[(class, (s,o))]])
        pair._2.foreach(tuple => // (class, Seq[(class, (s,o))])
          writeToHDFS(sc, tuple._2, pair._1 ++ "/" ++ tuple._1 ++ "_" ++ pair._1)
        )
      )

    // by subject and object
    val split3 = predicatePair.filter(pair => pair._1 != "type").
      map(pair =>
        (pair._1, pair._2.
           map(tuple =>
             ((findClass(tuple._1, classPair), findClass(tuple._2, classPair)),
              tuple)).
           groupBy(_._1)))

    split3.collect.
      foreach(pair => // (predicate, Map[(class, class), Seq[((class,class), (s,o))]])
        pair._2.foreach(tuple => // ((class, class), Seq[((class,class), (s,o))])
          writeToHDFS(sc, tuple._2,
                      pair._1 ++ "/" ++ tuple._1._1 ++
                      "_" ++ pair._1 ++ "_" ++ tuple._1._2)
        )
      )


    sc.stop()
  }

  def writeToHDFS(sc: SparkContext, seq: Seq[AnyRef], path: String) =
    sc.parallelize(seq).
      saveAsTextFile("hdfs://localhost:9000/user/jeremybi/" ++ path)

  def findClass(obj: String, map: Map[String, Seq[(String, String)]]) =
    map.foldLeft("noMatch")((flag, pair) =>
      if (pair._2 contains (pair._1, obj))
        pair._1
      else flag
    )

}
