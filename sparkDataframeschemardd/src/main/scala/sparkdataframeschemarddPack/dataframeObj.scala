package sparkdataframeschemarddPack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

object dataframeObj {
  
  case class columns(txnno:String,txndate:String,custno:String,amount:String,category:String,product:String,city:String,state:String,spendby:String)
  
   def main(args:Array[String]):Unit={
    
    val conf = new SparkConf().setAppName("First").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("ERROR")
					
		val spark=SparkSession.builder().getOrCreate()
		import spark.implicits._
		
		val data=sc.textFile("file:///C:/data/txns")
		val mapsplit=data.map(x=>x.split(","))
		val schemardd=mapsplit.map(x=>columns(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))
		val df = schemardd.toDF()
		df.show() //By default it only show 20 rows
		df.createOrReplaceTempView("txndf")
		val finaldata=spark.sql("select * from txndf where spendby='cash'")
		finaldata.show()
		 
}
}