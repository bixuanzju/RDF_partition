import org.apache.spark
import org.apache.spark._
import org.apache.spark.SparkContext._

object RDFPartitioner {

  def main(args: Array[String]) = {

    // Initialization
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("RDFPartition")

    val sc = new SparkContext(conf)
    val filePath = "/Users/jeremybi/Desktop/new_data/data/compress/part-r-00000"
    val typeHash = -1425683616493199L
    var fileNames = Nil: List[String]

    // Step 1
    // Divide by predicate
    // predicatePair is RDD[(predicate, [(subject, object)])]
    val predicatePair = sc.textFile(filePath).
      map(line => line.split(" ")).
      map(triple => (triple(1).toLong, (triple(0).toLong, triple(2).toLong))).
      groupByKey // TODO: partitionBy

    // Step 2
    // Subdivide by object in type predicate
    // classPair is Map[object, class]

    val classPredic = predicatePair.filter(pair => pair._1 == typeHash)

    val classPair  = sc.broadcast(classPredic.first._2.toMap)

    val classPairs =
      sc.parallelize(classPredic.first._2)
      .map(_.swap).groupByKey

    classPairs.collect.foreach(pair => {
                                 fileNames = ("ff" + pair._1) :: fileNames
                                 writeToHDFS(sc, pair._2, fileNames.head)
                               })

    val otherPredic = predicatePair.filter(pair => pair._1 != typeHash)

    // Step 3
    // Subdivide all predicates other than type predicate

    // by object
    val split1 = otherPredic.
      mapValues(pairSeq =>
        pairSeq.map(tuple => (findClass(tuple._2, classPair.value), tuple)).
          filter(pair => pair._1 != 0).
          groupBy(_._1))

    split1.collect.
      foreach(pair => // (predicate, Map[class, Seq[(class, (s,o))]])
        pair._2.foreach(tuple => // (class, Seq[(class, (s,o))])
          {
            fileNames = ("ff" + pair._1 + "_" + tuple._1) :: fileNames
            writeToHDFS(sc, tuple._2.map(_._2), fileNames.head)
          }))

    // by subject
    val split2 = otherPredic.
      mapValues(pairSeq =>
        pairSeq.map(tuple => (findClass(tuple._1, classPair.value), tuple)).
          filter(pair => pair._1 != 0).
          groupBy(_._1))

    split2.collect.
      foreach(pair => // (predicate, Map[class, Seq[(class, (s,o))]])
        pair._2.foreach(tuple => // (class, Seq[(class, (s,o))])
          {
            fileNames = ("ff" + tuple._1 + "_" + pair._1) :: fileNames
            writeToHDFS(sc, tuple._2.map(_._2), fileNames.head)
          }))

    // by subject and object
    val split3 = otherPredic.
      mapValues(pairSeq =>
        pairSeq.map(tuple =>
          ((findClass(tuple._1, classPair.value), findClass(tuple._2, classPair.value)), tuple)).
          filter {case ((c1, c2), _) => c1*c2 != 0}.
          groupBy(_._1))

    split3.collect.
      foreach(pair => // (predicate, Map[(class, class), Seq[((class,class), (s,o))]])
        pair._2.foreach(tuple => // ((class, class), Seq[((class,class), (s,o))])
          {
            fileNames = ("ff" + tuple._1._1 + "_" + pair._1 + "_" + tuple._1._2) :: fileNames
            writeToHDFS(sc, tuple._2.map(_._2), fileNames.head)
          }))

    // save all valid file names
    writeToHDFS(sc, fileNames, "filenames")

    sc.stop()
  }

  def writeToHDFS(sc: SparkContext, seq: Seq[Any], path: String) =
    sc.parallelize(seq).
      saveAsTextFile("hdfs://localhost:9000/user/jeremybi/partitions/" + path)

  def findClass(obj: Long, map: Map[Long, Long]): Long =
    map.get(obj) match {
      case Some(cls) => cls
      case None => 0
    }

}
