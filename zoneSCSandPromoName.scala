package Example

import java.text.SimpleDateFormat
import java.util.Calendar

import com.mongodb.spark.MongoSpark
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.bson.Document

import scala.util.Try

object zoneSCSandPromoName {


  def extractData(line: String)
  = {
    val fields = line.split(",")

    val df = new SimpleDateFormat("dd-MMM-yy")

    (Try(fields(5)) getOrElse(""),Try(fields(8)) getOrElse(""),Try(fields(9)) getOrElse(""),Try(fields(11)) getOrElse(""),Try(fields(12)) getOrElse(""),Try(fields(20)) getOrElse(""),
      Try(fields(21)) getOrElse(""),Try(fields(22)) getOrElse(""))

  }

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    val sc = new SparkContext("local[*]", "Monetary")

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("MongoSparkConnectorIntroCmd")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/SaleRepInsight.ZoneWiseStateWiseCityWiseSourceSiteNamesWisePromoName")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/SaleRepInsight.ZoneWiseStateWiseCityWiseSourceSiteNamesWisePromoName")
      .config("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
      .config("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
      .getOrCreate()



    val SaleRepdata = sc.textFile("../MacV_csvFiles/finalCsv/Sales Details.csv")

    val header = SaleRepdata.first() //extract header.It will be removed during streaming(42 and 44 line)

    val SaleRepdataWithoutHeader = SaleRepdata.filter(row => row != header)

    val AccFilter = SaleRepdataWithoutHeader.filter(x => x!= "")

    val mappedSaleRepdata = AccFilter.map(extractData)

    val totalfilter=mappedSaleRepdata.filter(s=> !s._8.isEmpty && !s._7.isEmpty && !s._6.isEmpty && !s._1.isEmpty && !s._4.isEmpty)

    val DownloaddataCount = mappedSaleRepdata.map(x => (x._8,x._7,x._6,x._1,x._4)).distinct().filter(s=> !s._5.isEmpty)

    //val finalResult=DownloaddataCount.map(s=>(s.split("@")(0),s.split("@")(1),s.split("@")(2),s.split("@")(3),s.split("@")(4)))


    //DownloaddataCount.foreach(println)


    val results = DownloaddataCount.collect()

    val today = Calendar.getInstance.getTime

    //  val df = new SimpleDateFormat("yyyy-MM-dd")

    results.foreach({ rdd =>

      val newDocs = Seq(new Document("TimeStamp",today).append("Zone",rdd._1).append("State",rdd._2).append("City",rdd._3).append("SourceSite",rdd._4).append("PromoName",rdd._5))

      MongoSpark.save(sc.parallelize(newDocs))

      // println(newDocs)

    })
  }


}
