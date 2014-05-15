import java.io._
import org.apache.spark
import org.apache.spark._
import org.apache.spark.SparkContext._

object RDFPartitioner {

  // Custom Accumulators for List[String]
  implicit object ListAP extends AccumulatorParam[List[String]] {
    def zero(v: List[String]) = List() : List[String]
    def addInPlace(v1: List[String], v2: List[String]) = v1 ++ v2
  }

  def main(args: Array[String]) = {

    // Initialization
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("RDFPartition")

    val sc = new SparkContext(conf)
    val filePath = "/Users/jeremybi/Desktop/new_data/data/compress/part-r-00000"
    val typeHash = -1425683616493199L
    // Accumulators for recording valid filenames
    val fileNames = sc.accumulator(List(): List[String])

    // Step 1
    // Divide by predicate
    // predicatePair is RDD[(predicate, [(subject, object)])]
    val predicatePair = sc.textFile(filePath)
      .map(line => line.split(" "))
      .map(triple => (triple(1).toLong, (triple(0).toLong, triple(2).toLong)))
      .groupByKey

    // Step 2
    // Subdivide by object in type predicate
    // classPair is Map[object, class]
    val classPredic = predicatePair.filter(pair => pair._1 == typeHash)

    val classPair  = sc.broadcast(classPredic.first._2.toMap)

    // classPairs is RDD[(pred, Map[class, Seq[(class, subject)]])]
    val classPairs = classPredic
      .map(pair => (pair._1, pair._2.map(_.swap).groupBy(_._1)))

    classPairs.foreach(pair => pair._2.foreach(
                         ppair => {
                           fileNames += List("ff" + ppair._1)
                           writeToHDFS(ppair._2.map(_._2).mkString("\n"),
                                       ("ff" + ppair._1))}))

    val otherPredic = predicatePair.filter(pair => pair._1 != typeHash)

    // Step 3
    // Subdivide all predicates other than type predicate

    // by object
    val split1 = otherPredic
      .mapValues(pairSeq =>
      pairSeq.map(tuple => (findClass(tuple._2, classPair.value), tuple))
        .filter(pair => pair._1 != 0)
        .groupBy(_._1))

    split1.foreach(pair => // (predicate, Map[class, Seq[(class, (s,o))]])
        pair._2.foreach(tuple => // (class, Seq[(class, (s,o))])
          {
            fileNames += List("ff" + pair._1 + "_" + tuple._1)
            writeToHDFS(tuple._2.map(_._2).mkString("\n"), ("ff" + pair._1 + "_" + tuple._1))
          }))

    // by subject
    val split2 = otherPredic
      .mapValues(pairSeq =>
        pairSeq.map(tuple => (findClass(tuple._1, classPair.value), tuple))
          .filter(pair => pair._1 != 0)
          .groupBy(_._1))

    split2. foreach(pair => // (predicate, Map[class, Seq[(class, (s,o))]])
        pair._2.foreach(tuple => // (class, Seq[(class, (s,o))])
          {
            fileNames += List("ff" + tuple._1 + "_" + pair._1)
            writeToHDFS(tuple._2.map(_._2).mkString("\n"), ("ff" + tuple._1 + "_" + pair._1))
          }))

    // by subject and object
    val split3 = otherPredic
      .mapValues(pairSeq =>
        pairSeq.map(tuple =>
          ((findClass(tuple._1, classPair.value), findClass(tuple._2, classPair.value)), tuple))
          .filter {case ((c1, c2), _) => c1*c2 != 0}
          .groupBy(_._1))

    split3. foreach(pair => // (predicate, Map[(class, class), Seq[((class,class), (s,o))]])
        pair._2.foreach(tuple => // ((class, class), Seq[((class,class), (s,o))])
          {
            fileNames += List("ff" + tuple._1._1 + "_" + pair._1 + "_" + tuple._1._2)
            writeToHDFS(tuple._2.map(_._2).mkString("\n"),
                        ("ff" + tuple._1._1 + "_" + pair._1 + "_" + tuple._1._2))
          }))

    // save all valid file names
    writeToHDFS(fileNames.value.mkString("\n"), "filenames")

    sc.stop()
  }

  def writeToHDFS(seq: String, path: String) =
    HDFSFileService.
      createNewHDFSFile("hdfs://localhost:9000/user/jeremybi/partitions/" + path, seq)

  def findClass(obj: Long, map: Map[Long, Long]): Long =
    map.get(obj) match {
      case Some(cls) => cls
      case None => 0
    }

}
