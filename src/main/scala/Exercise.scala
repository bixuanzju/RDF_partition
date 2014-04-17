import org.apache.spark
import org.apache.spark._
import org.apache.spark.SparkContext._

object RDFPartitioner {

  def main(args: Array[String]) = {

    // Initialization
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("Exercise")
      .setJars(List("/Users/jeremybi/scratch/scala/exercise/target/scala-2.10/rdf-partitioner-1.0.jar"))
      .setSparkHome("/Users/jeremybi/spark-0.9.1-bin-hadoop1")
    val sc = new SparkContext(conf)
    // val mapPath = "/Users/jeremybi/Desktop/standard_data/data/mapping/part-r-00000"
    val filePath = "/Users/jeremybi/Desktop/new_data/compress/part-r-00000"
    val typeHash = -1425683616493199L

    // val hashMap = sc.textFile(mapPath).
    //   map(line => line.split(" ")).
    //   map(array => (array(0).toLong, (array(1)))).collect.toMap

    // Step 1
    // Divide by predicate
    // predicatePair is RDD[(predicate, [(subject, object)])]
    val predicatePair = sc.textFile(filePath).
      map(line => line.split(" ")).
      map(triple => (triple(1).toLong, (triple(0).toLong, triple(2).toLong))).
      groupByKey

    // Step 2
    // Subdivide by object in type predicate
    // classPair is Map[object, class]
    val classPair  = sc.broadcast(predicatePair.filter {
                                    case (pred, _) => pred == typeHash
                                  }.first._2.toMap)

    val otherPredic = predicatePair.filter(pair => pair._1 != typeHash)


    val classPairs = predicatePair.
      filter {
        case (pred, _) => pred == typeHash
      }.first._2.
      map(truple => (truple._2, truple._1)).groupBy(_._1)

    classPairs.foreach(pair => writeToHDFS(sc, pair._2.map(_._2), "ff" + pair._1))

    // Step 3
    // Subdivide all predicates other than type predicate

    // by object
    val split1 = otherPredic.
      map(pair =>
        (pair._1, pair._2.
           map(tuple => (findClass(tuple._2, classPair.value), tuple)).
           groupBy(_._1)))

    split1.collect.
      foreach(pair => // (predicate, Map[class, Seq[(class, (s,o))]])
        pair._2.foreach(tuple => // (class, Seq[(class, (s,o))])
          writeToHDFS(sc, tuple._2.map(_._2), "ff" + pair._1 + "_" + tuple._1)))

    // by subject
    val split2 = otherPredic.
      map(pair =>
        (pair._1, pair._2.
           map(tuple => (findClass(tuple._1, classPair.value), tuple)).
           groupBy(_._1)))

    split2.collect.
      foreach(pair => // (predicate, Map[class, Seq[(class, (s,o))]])
        pair._2.foreach(tuple => // (class, Seq[(class, (s,o))])
          writeToHDFS(sc, tuple._2.map(_._2), "ff" + tuple._1 + "_" + pair._1)))

    // by subject and object
    val split3 = otherPredic.
      map(pair =>
        (pair._1, pair._2.
           map(tuple =>
             ((findClass(tuple._1, classPair.value), findClass(tuple._2, classPair.value)),
              tuple)).
           groupBy(_._1)))

    split3.collect.
      foreach(pair => // (predicate, Map[(class, class), Seq[((class,class), (s,o))]])
        pair._2.foreach(tuple => // ((class, class), Seq[((class,class), (s,o))])
          writeToHDFS(sc, tuple._2.map(_._2), "ff" + tuple._1._1 + "_" + pair._1 + "_" + tuple._1._2)))

    sc.stop()
  }

  def writeToHDFS(sc: SparkContext, seq: Seq[Any], path: String) =
    sc.parallelize(seq).
      saveAsTextFile("hdfs://localhost:9000/user/jeremybi/" + path)
      // saveAsTextFile("/Users/jeremybi/output.txt")

  def findClass(obj: Long, map: Map[Long, Long]): Long =
    map.get(obj) match {
      case Some(cls) => cls
      case None => -1
    }
}
