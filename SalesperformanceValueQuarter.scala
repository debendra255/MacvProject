package Example

import java.text.SimpleDateFormat
import java.util.Calendar

import com.mongodb.spark.MongoSpark
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.bson.Document

import scala.util.Try

object SalesperformanceValueQuarter {


  def extractData(line: String)
  = {
    val fields = line.split(",")

    val df = new SimpleDateFormat("dd-MMM-yy")

    (Try(fields(1)) getOrElse (""), Try(fields(18)) getOrElse (""))

  }

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    val sc = new SparkContext("local[*]", "Monetary")

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("MongoSparkConnectorIntroCmd")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/SaleRepInsight.SalesperformanceValueQuarter")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/SaleRepInsight.SalesperformanceValueQuarter")
      .config("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
      .config("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
      .getOrCreate()


    val SaleRepdata = sc.textFile("../MacV_csvFiles/finalCsv/Sales Details.csv")

    val header = SaleRepdata.first() //extract header.It will be removed during streaming(42 and 44 line)

    val SaleRepdataWithoutHeader = SaleRepdata.filter(row => row != header)

    val AccFilter = SaleRepdataWithoutHeader.filter(x => x != "")

    val mappedSaleRepdata = AccFilter.map(extractData)

    val TotalFilter = mappedSaleRepdata.filter(s=> !s._1.isEmpty && !s._2.isEmpty)

    val checkyear17 = TotalFilter.map(s=>(s._1.toString.substring(3,5),s._1.toString.substring(6,10),s._2)).map(s=>(s._1+"@"+s._3,if(s._2=="2016") {"0"} else ("1"))).filter(s=>(s._2!="0")).map(s=>(s._1.split("@")(0),s._1.split("@")(1)))

    val TotalFiltercount=checkyear17.map(s=>(s._1.toString,s._2.toInt)).reduceByKey(_+_)

    val placeTotalFiltercountQuarter=TotalFiltercount.map(s=>(s._2+"@"+s._1,s._1.toInt))

    val substringplaceTotalFiltercountQuarter=placeTotalFiltercountQuarter.map(s=>(s._1, s._2.toString.substring(0))).map(s=>(s._1,s._2.toInt))

    val ChecksubstringInQuarter=substringplaceTotalFiltercountQuarter.map(s=>(s._1,if(s._2 >9){"Qtr4"} else if(s._2>6 && s._2<=9){"Qtr3"} else if(s._2>3 && s._2<=6){"Qtr2"} else {"Qtr1"}))


    val finalResult=ChecksubstringInQuarter.map(s=>(s._1.split("@")(0),s._2)).map(s=>(s._2,s._1))






    val results = finalResult.collect()

    val today = Calendar.getInstance.getTime

    val df = new SimpleDateFormat("dd-MM-yyyy")

    results.foreach({ rdd =>

      val newDocs = Seq(new Document("TimeStamp",today).append("Quarter",rdd._1).append("Amount",rdd._2))

     MongoSpark.save(sc.parallelize(newDocs))

       //println(newDocs)

    })
  }


}
