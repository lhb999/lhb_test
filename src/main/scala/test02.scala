import java.io.{File, PrintWriter}
import scala.collection.mutable
import scala.io.Source

object test02 {
  case class SAM4(qname:String, flag:String, rname:String, pos:Long, others:Array[String])
  def markDup(flag:Int) = { flag | 0x400 }
  def markDups(sam:SAM4) = {
    val marked = markDup(sam.flag.toInt).toString
    val rebuild = SAM4(sam.qname, marked, sam.rname, sam.pos, sam.others)
    rebuild
  }
  def parseDBG(str:String) : String = {
    val srcData = str
    val spl = str.split("\\s+")

    if(spl.length < 4) {
      return "DBG less string: "+str
    }

    val qname = spl(0)
    val flag = spl(1)
    val rname = spl(2)


    val pos1 = spl(3)
    val pos = try {
      pos1.toLong
    } catch {
      case e:Exception => {
        e.printStackTrace()
        0L
      }
    }
    val others = spl.slice(4, spl.length)

    val t1 = SAM4(qname, flag, rname, pos, others)
    val t2 = getString(t1)

    t2
  }
  def parseToSAM(str:String) : SAM4 = {
    val srcData = str
    val spl = str.split("\\s+")

    val qname = spl(0)
    val flag = spl(1)
    val rname = spl(2)
    val pos1 = spl(3)
    val pos = try {
      pos1.toLong
    } catch {
      case e:Exception => {
        e.printStackTrace()
        987654321L
      }
    }

    val others = spl.slice(4, spl.length)

    SAM4(qname, flag, rname, pos, others)
  }
  def getString(sam:SAM4) = {
    val arr:Array[String] = Array(sam.qname, sam.flag, sam.rname, sam.pos.toString) ++ sam.others
    val res = s"${arr.mkString("\t")}"
    res
  }
  def checkFlag(bit1:Int, bit2:Int) : Boolean = {
    val res =  ((bit1 & bit2) != 0)
    res
  }
  def isUnmapped(bit:Int) = {
    val res = checkFlag(bit, 0x4)
  }
  def readFirst(path:String, prefix:String="ERR") = { //SBL
    var isFirst = false
    //  val hashMap = new mutable.HashMap[Long, SAM4]()
    val list = new mutable.ArrayBuffer[(Long, SAM4)]()
    //  val list = new mutable.ArrayBuffer[(Long, SAM4)]()
    var cnt = 0
    var key = 0L

    for(line <- Source.fromFile(path).getLines()){
      if(isFirst == true) {
        if(line.startsWith(prefix)) {
          val samLine = parseToSAM(line)
          val qname = line.split("\t").head.trim()
          list.+=((key, samLine))
          if((cnt % 100000) == 0) println(cnt)
          cnt += 1
          //println("add : "+qname+" cnt : "+cnt)
          key = 0L
        }
        isFirst = false
      }
      if(line.startsWith("first")) {
        isFirst = true
        key = line.split(":").last.trim().toLong
      }
    }
    val res = (list, cnt)
    res
  } // SBL
  def readFirst2(path:String, prefix:String="ERR") = { //SBL1
    var isFirst = false
    //  val hashMap = new mutable.HashMap[Long, SAM4]()
    val list = new mutable.ArrayBuffer[(Long, SAM4)]()
    //  val list = new mutable.ArrayBuffer[(Long, SAM4)]()
    var cnt = 0
    var key = 0L
    var qnameStack = 0
    var lastQname = ""

    for(line <- Source.fromFile(path).getLines()){
      if(line.startsWith("first")) {
        key = line.split(":").last.trim().toLong
        qnameStack = 0
      }
      else if(line.startsWith(prefix)) {
        val samLine = parseToSAM(line)
        if(!lastQname.isEmpty() && samLine.qname != lastQname) {

        }
        if((cnt % 100000) == 0) println(cnt)
        if(qnameStack >= 2) {
          if(lastQname == samLine.qname) list.+=((key, samLine))
          else list.+=((0L, samLine))
        }
        else list.+=((key, samLine))
        cnt += 1
        qnameStack += 1
        //println("add : "+qname+" cnt : "+cnt)
        key = 0L
        lastQname = samLine.qname
      }
    }
    val res = (list, cnt)
    res
  } // SBL

  def readFirst3(path:String, prefix:String = "ERR000954",firstQname:String = "ERR000954.1",outFunc: String => Unit = println) = {//SBL
    var arr = new mutable.ArrayBuffer[String]()
    var lastQname = firstQname
    for(line <- Source.fromFile(path).getLines()){
      if(line.startsWith("first")) {
        val key = line.split(":").last.trim()
        arr.+=(key)
      }
      else if(line.startsWith(prefix)){
        val sam = parseToSAM(line)
        if(lastQname != sam.qname) {
          val str = arr.mkString("\t")
          outFunc(str)
          arr.clear()
          if(arr.isEmpty) {
            arr.+=("0")
          }
          arr.+=(sam.qname, sam.flag)
        }
        else if(lastQname == sam.qname) {
          if(arr.contains(sam.qname)) {
            arr.+=(sam.flag)
          }
          else {
            arr.+=(sam.qname)
            arr.+=(sam.flag)
          }
        }
        lastQname = sam.qname
      }
    }
  }

  def readSecond(path:String) = {
    //  val hashMap = new mutable.HashMap[String, Long]()
    val list = new mutable.ArrayBuffer[(Long, SAM4)]()
    //  val list = new mutable.ArrayBuffer[(Long, SAM4)]()
    var cnt = 0
    var key = 0L

    for(nline <- Source.fromFile(path).getLines()){
      if(nline.charAt(0).isDigit) {
        val spl = nline.split("XXQQ")
        //      if(spl(0).length > 25)
        key = spl(0).toLong
        val line = parseToSAM(spl(1))
        //      val tmpLen = spl.length
        //      for(x <- 1 to tmpLen) {
        //        val line = parseToSAM(spl(x))
        //      }
        if((cnt % 100000) == 0) println(cnt)
        cnt += 1
        //      println("add : "+qname+" cnt : "+cnt)
        list.+=((key, line))
      }
    }
    val res = (list, cnt)
    res
  } //XXQQ
  def testfunc(path:String) = {
    val list = new mutable.ArrayBuffer[String]()
    for(line <- Source.fromFile(path).getLines()){
      list.+=(line)
    }
    list
  }
  def main(args: Array[String]): Unit = {
    // file1 -> res1.out  [key XXQQ ERR~]
    // file2 -> target.sbl [
    //    val file1 = """/home/dblab/NextGen/BigBWA_zmq_tmp/src/main/native/bwa-0.7.15/test-data/target.sbl""" //samblaster out

//    val file4 = """/home/dblab/NextGen/BigBWA_zmq_tmp/src/main/native/bwa-0.7.15/test-data/test4.tmp"""
//    val rest = testfunc(file4)

//    val file3 = """/home/dblab/NextGen/BigBWA_zmq_tmp/src/main/native/bwa-0.7.15/test-data/test6.tmp""" //XXQQ
//    val writer = new PrintWriter(new File(file3 ))

    val file1 = """/home/dblab/NextGen/BigBWA_zmq_tmp/src/main/native/bwa-0.7.15/test-data/target.sbl""" //samblaster out
    val f1 = readFirst3(file1)
//    val f1 = readFirst(file1)

    //    val file2 = """/home/dblab/NextGen/BigBWA_zmq_tmp/src/main/native/bwa-0.7.15/test-data/test2.out""" //XXQQ
    //    val f2 = readSecond(file2)

    //    f2._1.filter(x => x._1 == 9488385917957663L)
    //    val f2 = readSecond(file2)
    //    var cnt = 0


//      var isFirst = false
//      //  val hashMap = new mutable.HashMap[Long, SAM4]()
//      val list = new mutable.ArrayBuffer[(Long, SAM4)]()
//      //  val list = new mutable.ArrayBuffer[(Long, SAM4)]()
//      var cnt = 0
//      var key = 0L
//
//      for(line <- Source.fromFile(file1).getLines()){
//        if(isFirst == true) {
//          if(line.startsWith("ERR")) {
//            val samLine = parseToSAM(line)
//            val qname = line.split("\t").head.trim()
//            if(rest.contains(qname)) {
//              println(line)
//              writer.println(line)
//            }
//            if((cnt % 100000) == 0) println(cnt)
//            cnt += 1
//            //println("add : "+qname+" cnt : "+cnt)
//            key = 0L
//          }
//          isFirst = false
//        }
//      }


//    rest.foreach {qname =>
//      val res = f1._1.filter(x => x._2.qname == qname)
//      val mapped = res.map{x =>
//        val str = s"${x._2.qname}\t${x._2.flag}\t${x._2.pos}"
//        str
//      }
//      writer.write(mapped.mkString("\n"))
//    }
  }
}
