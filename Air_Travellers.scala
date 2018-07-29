import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession

object Air_Travellers {
     def main(args: Array[String]) { 
       //case class Row(id:Int, Origin:String, Destination:String, Medium:String, Cost:Int, Year:Int)
       
       //Create spark object       
       val spark = SparkSession
                     .builder()
                     .appName("Spark Practice")
                     .config("spark.master", "local")
                     .getOrCreate()
                     
       val df = spark.read.csv("Input_dataset//Dataset_Holidays.txt")
       val colNames = Seq("ID", "Origin", "Destination", "Medium", "Cost", "Year")
       val df_holidays = df.toDF(colNames: _*)       
       
       val df_users = spark.read.csv("Input_dataset//Dataset_User_details.txt").toDF("ID","name","Age")
       
       //Join two dataframes :
       //1 : This will create duplicated columns
       //val df_join = df_holidays.join(df_users, df_holidays.col("ID") === df_users.col("ID"))
       
       //2 : This will removed duplicated columns while join
       val df_join = df_holidays.join(df_users, Seq("ID"))
       
       // Create a temp table from dataframe to perform SQL queries
       df_join.registerTempTable("joined_df")
       
       // Perform computation on temp table using SQL queries
       val count_lt_20 = spark.sqlContext.sql("SELECT COUNT(*) AS count FROM joined_df WHERE Age<20")
                           .first()
                           .get(0)
                           .##()
       
       val count_bw_20_35 = spark.sqlContext.sql("SELECT COUNT(*) AS count FROM joined_df WHERE Age>=20 AND Age<=35")
                           .first()
                           .get(0)
                           .##()
       
       val count_gt_35 = spark.sqlContext.sql("SELECT COUNT(*) AS count FROM joined_df WHERE Age>35")
                           .first()
                           .get(0)
                           .##()
      
      // Create a DataFrame using the above computed data
      val TravellingCount = Seq(
        Row("<20", count_lt_20),
        Row("20-35", count_bw_20_35),
        Row(">35", count_gt_35)
      )
      
      val TravellingSchema = List(
        StructField("AgeGroup", StringType),
        StructField("TravellingCount", IntegerType)
      )
      
      val TravellingDF = spark.createDataFrame(
        spark.sparkContext.parallelize(TravellingCount),
        StructType(TravellingSchema)
      )
      
      // Create a temp table and calculate the Age-Group having highest travellers      
      TravellingDF.registerTempTable("TravellingDF_temp")
      spark.sqlContext.sql("SELECT AgeGroup as Age_Group, TravellingCount as Max_Travellings FROM TravellingDF_temp WHERE TravellingCount=(SELECT MAX(TravellingCount) FROM TravellingDF_temp)").show() 
     }
}