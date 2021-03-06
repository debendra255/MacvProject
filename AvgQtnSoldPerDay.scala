package Example

import java.text.SimpleDateFormat
import java.util.Calendar

import com.mongodb.spark.MongoSpark
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.bson.Document

import scala.util.Try

object AvgQtnSoldPerDay {

  def extractData(line: String)
  = {
    val fields = line.split(",")

    val df = new SimpleDateFormat("dd-MMM-yy")

    (Try(fields(0)) getOrElse(""),Try(fields(1)) getOrElse(""),Try(fields(5)) getOrElse(""),Try(fields(17)) getOrElse(""))

  }

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    val sc = new SparkContext("local[*]", "Monetary")

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("MongoSparkConnectorIntroCmd")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/SaleRepInsight.PoscodeWisePosNameWiseQtn")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/SaleRepInsight.PoscodeWisePosNameWiseQtn")
      .config("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
      .config("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
      .getOrCreate()



    val SaleRepdata = sc.textFile("../MacV_csvFiles/finalCsv/Sales Details.csv")

    val header = SaleRepdata.first() //extract header.It will be removed during streaming(42 and 44 line)

    val SaleRepdataWithoutHeader = SaleRepdata.filter(row => row != header)

    val AccFilter = SaleRepdataWithoutHeader.filter(x => x!= "")

    val mappedSaleRepdata = AccFilter.map(extractData)

    val TotalFilter = mappedSaleRepdata.filter(s=> !s._1.isEmpty && !s._2.isEmpty && !s._3.isEmpty && !s._4.isEmpty)

    val checkyear17 = TotalFilter.map(s=>(s._1+"@"+s._2+"@"+s._3+"@"+s._4,s._2.toString.substring(6,10))).map(s=>(s._1,if(s._2=="2016") {"0"} else ("1"))).filter(s=>(s._2!="0")).map(s=>(s._1.split("@")(0),s._1.split("@")(1),s._1.split("@")(2),s._1.split("@")(3)))

    val finalDatacheckyear17=checkyear17.map(s=>(s._1,s._2,s._3,s._4))//poscode,date,sourcesite,qtn

    val DownloaddataCount = finalDatacheckyear17.map(x => (x._1+"@"+x._3,x._4.toInt)).reduceByKey(_+_)//.map(s=>(s._1.split("@")(0),s._1.split("@")(1)+"@"+s._2))


    //DownloaddataCount.foreach(println)

    val distinctCountPosCodePerDay=finalDatacheckyear17.map(x=>(x._1,x._2,x._3)).distinct().map(s=>(s._1+"@"+s._3)).map(s=>(s,1)).reduceByKey(_+_)

    val twoCollectionjoin=distinctCountPosCodePerDay.join(DownloaddataCount)

    val finalResult=twoCollectionjoin.map(s=>(s._1.split("@")(0),s._1.split("@")(1),s._2._1,s._2._2)).map(s=>(s._1,s._2,s._4.toInt,s._3.toInt)).map(s=>(s._1,s._2,s._3/s._4))

   // finalResult.foreach(println)


    val results = finalResult.collect()

    val today = Calendar.getInstance.getTime

    //  val df = new SimpleDateFormat("yyyy-MM-dd")

    results.foreach({ rdd =>

      val newDocs = Seq(new Document("TimeStamp",today).append("PosCode",rdd._1).append("SourceSite",rdd._2).append("QtnPerDay",rdd._3))

     MongoSpark.save(sc.parallelize(newDocs))

     // println(newDocs)

    })
  }



}
