import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by christophebovigny on 01.12.15.
 */
object QueryMolecules {

  def tanimoto(query: Array[Byte], database: Array[Byte]): Float = {

    var z = 0;
    var z2 = 0;


    query.zip(database).foreach((x: (Byte, Byte)) => {
      z += numberOfBitsSet((x._1 & x._2).toByte)
      z2 += numberOfBitsSet((x._1 | x._2).toByte)
    })

    (z: Float) / z2
  }

  def numberOfBitsSet(b: Byte) : Int = (0 to 7).map((i : Int) => (b >>> i) & 1).sum

  def main(args: Array[String]) {

    if (args.length < 2) {
      println("Usage: " + this.getClass.getSimpleName +
        "<file1> <queryinput>")
      System.exit(1)
    }


    val Array(file1: String, userInputStr: String) = args

    val userInput = HexBytesUtil.hex2bytes(userInputStr)

    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    //.set("spark.cassandra.connection.host", cassandraSeed)

    val sc = new SparkContext(conf)

    val smileAndFingerPrint = sc.textFile(file1).map(line => {
      val v = line.split("\\s+");
      (v(0).trim, HexBytesUtil.hex2bytes(v(1).trim))

    })


     val tani =  smileAndFingerPrint.map(b => {
        val t = (tanimoto(b._2, userInput), b); //println(t);
       t
      })
      .filter( t => t._1 >= 0.8)
      .sortByKey(false)
      .mapPartitions(p => {
        var i: Long = 0;
        p.map(o => {
          i += 1
          if (i <= 3) {
            o
          } else {
            null
          }
        })
      }).filter(_ != null)

    val topN = tani.take(100)

    topN.foreach(b => println("(" + b._1 + "," + b._2._1 + ")"))


  }





}
