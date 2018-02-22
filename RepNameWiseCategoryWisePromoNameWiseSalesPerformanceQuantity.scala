package Example

import java.sql.Date
import java.text.SimpleDateFormat
import java.util.Calendar

import com.mongodb.spark.MongoSpark
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.bson.Document

import scala.util.Try

object RepNameWiseCategoryWisePromoNameWiseSalesPerformanceQuantity {


  def extractData(line: String)
  = {
    val fields = line.split(",")

    val df = new SimpleDateFormat("dd-MMM-yy")

    (Try(fields(1)) getOrElse (""), Try(fields(17)) getOrElse (""),Try(fields(8)) getOrElse (""), Try(fields(9)) getOrElse (""),Try(fields(11)) getOrElse (""), Try(fields(12)) getOrElse (""))

  }

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    val sc = new SparkContext("local[*]", "Monetary")

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("MongoSparkConnectorIntroCmd")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/SaleRepInsight.RepNameWiseCategoryWisePromoNameWiseSalesPerformanceQuantity")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/SaleRepInsight.RepNameWiseCategoryWisePromoNameWiseSalesPerformanceQuantity")
      .config("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
      .config("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
      .getOrCreate()


    val SaleRepdata = sc.textFile("../MacV_csvFiles/finalCsv/Sales Details.csv")

    val header = SaleRepdata.first() //extract header.It will be removed during streaming(42 and 44 line)

    val SaleRepdataWithoutHeader = SaleRepdata.filter(row => row != header)

    val AccFilter = SaleRepdataWithoutHeader.filter(x => x != "")

    val mappedSaleRepdata = AccFilter.map(extractData)

    val TotalFilter = mappedSaleRepdata.filter(s=> !s._1.isEmpty && !s._2.isEmpty && !s._3.isEmpty && !s._4.isEmpty && !s._5.isEmpty && !s._6.isEmpty)

    val checkyear17 = TotalFilter.map(s=>(s._2,s._1.toString.substring(6,10),s._1+"@"+s._3+"@"+s._4+"@"+s._5+"@"+s._6)).map(s=>(s._1,s._3,if(s._2=="2016") {"0"} else ("1"))).filter(s=>(s._3!="0"))//.map(s=>(s._1.split("@")(0),s._1.split("@")(1)))

    //checkyear17.foreach(println)
    val TotalFiltercount=checkyear17.map(s=>(s._2,s._1.toInt)).reduceByKey(_+_)

    //TotalFiltercount.foreach(println)
    val finalResult=TotalFiltercount.map(s=>(s._1.split("@")(0),s._1.split("@")(1),s._1.split("@")(2),s._1.split("@")(4),s._1.split("@")(3),s._2))
    val results = finalResult.collect()

    val today = Calendar.getInstance.getTime

    val df = new SimpleDateFormat("dd-MM-yyyy")

    results.foreach({ rdd =>

     // val newDocs = Seq(new Document("TimeStamp",today).append("date",new Date(df.parse(rdd._1).getTime)).append("SourceSite",rdd._2).append("City",rdd._3).append("State",rdd._4).append("Category",rdd._5).append("Count",rdd._6))

      val newDocs = Seq(new Document("TimeStamp",today).append("date",new Date(df.parse(rdd._1).getTime)).append("SalesRepNameid",rdd._2).append("SalesRepName",rdd._3).append("PromoName",rdd._5).append("Category",rdd._4).append("Count",rdd._6))

      MongoSpark.save(sc.parallelize(newDocs))

      // println(newDocs)

    })
  }

}
