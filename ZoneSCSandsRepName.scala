package Example

import java.text.SimpleDateFormat
import java.util.Calendar

import com.mongodb.spark.MongoSpark
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.bson.Document

import scala.util.Try

object ZoneSCSandsRepName {


  def extractData(line: String)
  = {
    val fields = line.split(",")

    val df = new SimpleDateFormat("dd-MMM-yy")

    (Try(fields(5)) getOrElse(""),Try(fields(8)) getOrElse(""),Try(fields(9)) getOrElse(""),Try(fields(11)) getOrElse(""),Try(fields(12)) getOrElse(""),Try(fields(20)) getOrElse(""),
      Try(fields(21)) getOrElse(""),Try(fields(22)) getOrElse(""),Try(fields(1)) getOrElse(""))

  }

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    val sc = new SparkContext("local[*]", "Monetary")

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("MongoSparkConnectorIntroCmd")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/SaleRepInsight.ZoneWiseStateWiseCityWiseSourceSiteNamesSalesRepName")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/SaleRepInsight.ZoneWiseStateWiseCityWiseSourceSiteNamesSalesRepName")
      .config("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
      .config("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
      .getOrCreate()



    val SaleRepdata = sc.textFile("../MacV_csvFiles/finalCsv/Sales Details.csv")

    val header = SaleRepdata.first() //extract header.It will be removed during streaming(42 and 44 line)

    val SaleRepdataWithoutHeader = SaleRepdata.filter(row => row != header)

    val AccFilter = SaleRepdataWithoutHeader.filter(x => x!= "")

    val mappedSaleRepdata = AccFilter.map(extractData)



    val checkyear17 = mappedSaleRepdata.map(x=>(x._8+"@"+x._7+"@"+x._6+"@"+x._1+"@"+x._2+"@"+x._3,x._9.toString.substring(6,10))).map(s=>(s._1,if(s._2=="2016") {"0"} else ("1"))).filter(s=>(s._2!="0")).map(s=>(s._1))


    val DownloaddataCount = checkyear17.map(x => (x)).distinct()//.count()//.filter(s=> !( s._1 == IntegerType))

   // print("dddddddddddddddddddddddddddddddddddddddd",DownloaddataCount)

   val finalResult=DownloaddataCount.map(s=>(s.split("@")(0),s.split("@")(1),s.split("@")(2),s.split("@")(3),s.split("@")(4),s.split("@")(5)))

   // DownloaddataCount.foreach(println)


   val results = finalResult.collect()

    val today = Calendar.getInstance.getTime

    //  val df = new SimpleDateFormat("yyyy-MM-dd")

    results.foreach({ rdd =>

      val newDocs = Seq(new Document("TimeStamp",today).append("Zone",rdd._1).append("State",rdd._2).append("City",rdd._3).append("SourceSite",rdd._4.toString).append("SalesRepNameid",rdd._5).append("SalesRepName",rdd._6))

      MongoSpark.save(sc.parallelize(newDocs))

      //println(newDocs)

    })
  }



}
