import java.io.BufferedInputStream
import java.io.File
import java.io.FileInputStream
import java.io.InputStream
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import org.apache.hadoop.io.IOUtils;

object HDFSFileService {
  private val conf = new Configuration()
  private val hdfsCoreSitePath = new Path("/usr/local/Cellar/hadoop121/1.2.1/libexec/conf/core-site.xml")
  private val hdfsHDFSSitePath = new Path("/usr/local/Cellar/hadoop121/1.2.1/libexec/conf/hdfs-site.xml")

  conf.addResource(hdfsCoreSitePath)
  conf.addResource(hdfsHDFSSitePath)

  private val fileSystem = FileSystem.get(conf)

  def saveFile(filepath: String): Unit = {
    val file = new File(filepath)
    val out = fileSystem.create(new Path(file.getName))
    val in = new BufferedInputStream(new FileInputStream(file))
    var b = new Array[Byte](1024)
    var numBytes = in.read(b)
    while (numBytes > 0) {
      out.write(b, 0, numBytes)
      numBytes = in.read(b)
    }
    in.close()
    out.close()
  }

  def createNewHDFSFile(filepath: String, content: String) {

    val path = new Path(filepath)
    val uriBackUp = "hdfs://localhost:9000/user/jeremybi/backup.tmp"

    if (fileSystem.exists(path)) {
      val out = fileSystem.create(new Path(uriBackUp), true);
      val in = fileSystem.open(path)

      IOUtils.copyBytes(in, out, 4096, false);

      out.writeBytes("\n"+ content)
      in.close
      out.close

      removeFile(filepath)
      renameFile(uriBackUp, filepath)
    }
    else {
      val out = fileSystem.create(path)
      out.write(content.getBytes("UTF-8"))
      out.close()
    }
  }

  def removeFile(filename: String): Boolean = {
    val path = new Path(filename)
    fileSystem.delete(path, true)
  }

  def renameFile(src: String, dst: String): Boolean = {
    val psrc = new Path(src)
    val pdst = new Path(dst)
    fileSystem.rename(psrc, pdst)
  }

  def getFile(filename: String): InputStream = {
    val path = new Path(filename)
    fileSystem.open(path)
  }

  def createFolder(folderPath: String): Unit = {
    val path = new Path(folderPath)
    if (!fileSystem.exists(path)) {
      fileSystem.mkdirs(path)
    }
  }
}
