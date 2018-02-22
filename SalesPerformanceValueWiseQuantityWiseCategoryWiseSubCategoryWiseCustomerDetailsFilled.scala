package Example

import java.sql.Date
import java.text.SimpleDateFormat
import java.util.Calendar

import com.mongodb.spark.MongoSpark
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.bson.Document

import scala.util.Try

object SalesPerformanceValueWiseQuantityWiseCategoryWiseSubCategoryWiseCustomerDetailsFilled {


  def extractData(line: String)
  = {
    val fields = line.split(",")

    val df = new SimpleDateFormat("dd-MMM-yy")

    (Try(fields(1)) getOrElse (""), Try(fields(2)) getOrElse (""), Try(fields(7)) getOrElse (""),
      Try(fields(12)) getOrElse (""), Try(fields(13)) getOrElse (""), Try(fields(17)) getOrElse (""), Try(fields(18)) getOrElse (""))

  }

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    val sc = new SparkContext("local[*]", "Monetary")

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("MongoSparkConnectorIntroCmd")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/SaleRepInsight.SalesPerformanceValueWiseQuantityWiseCategoryWiseSubCategoryWiseCustomerDetailsFilled")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/SaleRepInsight.SalesPerformanceValueWiseQuantityWiseCategoryWiseSubCategoryWiseCustomerDetailsFilled")
      .config("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
      .config("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
      .getOrCreate()


    val SaleRepdata = sc.textFile("../MacV_csvFiles/finalCsv/Sales Details.csv")

    val header = SaleRepdata.first() //extract header.It will be removed during streaming(42 and 44 line)

    val SaleRepdataWithoutHeader = SaleRepdata.filter(row => row != header)

    val AccFilter = SaleRepdataWithoutHeader.filter(x => x != "")

    val mappedSaleRepdata = AccFilter.map(extractData)

    val TotalFilter = mappedSaleRepdata.filter(s=> !s._1.isEmpty && !s._2.isEmpty && !s._3.isEmpty && !s._4.isEmpty && !s._5.isEmpty && !s._6.isEmpty && !s._7.isEmpty )

    val checkyear17 = mappedSaleRepdata.map(s=>(s._1,s._1.toString.substring(6,10),s._2+"#@"+s._3+"#@"+s._4+"#@"+s._5+"#@"+s._6+"#@"+s._7)).map(s=>(s._1+"#@"+s._3,if(s._2=="2016") {"0"} else ("1"))).filter(s=>(s._2!="0"))//.map(s=>(s._1.split("#")(1),s._1.split("#")(2),s._1.split("#")(3),s._1.split("#")(4),s._1.split("#")(5),s._1.split("#")(6)))

    //date,billNo,phoneno.,category,subCategory,QTY,Amount,

    val data17=checkyear17.map(s=>(s._1)).map(s=>(s.split("#@")(0),s.split("#@")(1),s.split("#@")(2),s.split("#@")(3),s.split("#@")(4),s.split("#@")(5),s.split("#@")(6)))

   // data17.foreach(println)

    val FirstAddQtrsalesperformanceValueAndQuantityAndCatAndSubCat=data17.map(s=>(s._1+"#@"+s._4+"#@"+s._5,s._6.toString.toLong)).reduceByKey(_+_).filter(s=> !s._1.isEmpty)

    val FirstAddValuesalesperformanceValueAndQuantityAndCatAndSubCat=data17.map(s=>(s._1+"#@"+s._4+"#@"+s._5,s._7.toString.toLong)).reduceByKey(_+_).filter(s=> !s._1.isEmpty)

    val JoinTwoCollection=FirstAddQtrsalesperformanceValueAndQuantityAndCatAndSubCat.join(FirstAddValuesalesperformanceValueAndQuantityAndCatAndSubCat).map(s=>(s._1.toString,s._2._1+"#@"+s._2._2))

   // JoinTwoCollection.foreach(println)


   val DistinctmappedSaleRepdata=data17.map(s=>(s._1+"#@"+s._4+"#@"+s._5+"#@"+s._2,s._3)).distinct()//.count()

    val toCheckCustomerMobile = DistinctmappedSaleRepdata.map(s => (s._1,if(s._2.length == 12 || s._2.length==10 || s._2.length==11){1} else (0)))//.map(s=>(s._2+"@"+s._3+"@"+s._4+"@"+s._5,s._6))

    val test=toCheckCustomerMobile.map(s=>(s._1,s._2)).map(s=>(s._1.split("#@")(0)+"#@"+s._1.split("#@")(1)+"#@"+s._1.split("#@")(2),s._2.toInt)).reduceByKey(_+_)//.filter(s=> !s._1.isEmpty)

    val toCheckCustomerMobileYesOrNo=test.map(s=>(s._1,if(s._2>=1) {"Yes"} else ("No"))).map(s=>(s._1+"#@"+s._2)).map(s=>(s,1)).reduceByKey(_+_)

    val SplittoCheckCustomerMobileYesOrNo=toCheckCustomerMobileYesOrNo.map(s=>(s._1.split("#@")(0),s._1.split("#@")(1),s._1.split("#@")(2),s._1.split("#@")(3),s._2))

    val DistinctYesPerNo=SplittoCheckCustomerMobileYesOrNo.map(s=>(s._1+"#@"+s._2+"#@"+s._3,s._4)).distinct()

    val countMobileyesAndNo=SplittoCheckCustomerMobileYesOrNo.map(s=>(s._1+"#@"+s._2+"#@"+s._3,s._5.toInt)).reduceByKey(_+_)

    val JoinMobileNumber=DistinctYesPerNo.join(countMobileyesAndNo).map(s=>(s._1.toString,s._2._1+"#@"+s._2._2))

    val JoinTotalCollections=JoinTwoCollection.join(JoinMobileNumber).map(s=>(s._1,s._2._1,s._2._2))

    //print("ddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",JoinTotalCollections.count())


   // JoinTotalCollections.foreach(println)

    val finalResult=JoinTotalCollections.map(s=>(s._1.split("#@")(0),s._1.split("#@")(1),s._1.split("#@")(2),s._2.split("#@")(0),s._2.split("#@")(1),s._3.split("#@")(0),s._3.split("#@")(1)))

    val results = finalResult.collect()

    //results.foreach(println)

    val today = Calendar.getInstance.getTime

    val df = new SimpleDateFormat("dd-MM-yyyy")
    results.foreach({ rdd =>

      val newDocs = Seq(new Document("TimeStamp",today).append("date",new Date(df.parse(rdd._1).getTime)).append("Category",rdd._2).append("SubCategory",rdd._3).append("Quantity",rdd._4).append("Amount",rdd._5).append("CustomerMobile",rdd._6).append("Count",rdd._7))

     MongoSpark.save(sc.parallelize(newDocs))

      //println(newDocs)

    })
  }



}
