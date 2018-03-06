package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
  
  var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
  pickupInfo.createOrReplaceTempView("nyctaxitrips")
  pickupInfo.show()

  
  spark.udf.register("CalculateX",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
  spark.udf.register("CalculateY",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
  spark.udf.register("CalculateZ",(pickupTime: String)=>((
    HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))
  pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
  var newCoordinateName = Seq("x", "y", "z")
  pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
  pickupInfo.createOrReplaceTempView("dataframe")



  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31

  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

  var map_dataframe = spark.sql("SELECT x , y, z, count(1)  AS xj from dataframe where x<="+maxX+" and x>="+minX+" and y<="+maxY+" and y>="+minY+" and z<="+maxZ+" and z>="+minZ+"  group by x, y, z").persist()
  map_dataframe.createOrReplaceTempView("map_dataframe")

  var summation_data = spark.sql("SELECT sum(xj) as sumXj,sum(xj*xj) as sumXjSqr from map_dataframe").persist()

  var summationXj = summation_data.first.getLong(0)

  var squareroot_summationXj = summation_data.first.getLong(1)

  var mean = summationXj.toDouble/numCells

  var standard_dev = math.sqrt(((squareroot_summationXj.toDouble/numCells)-(mean * mean)))



  spark.udf.register("get_getis_score",(weight: Int, xbar: Double,std_dev:Double,count_neighbors:Int,count_cells:Int)=>((
   HotcellUtils.get_getis_score(weight,xbar,std_dev,count_neighbors,count_cells)
      )))


  var neighbor_dataframe = spark.sql("select a.x, a.y, a.z , sum(b.xj)   as weight , count(1) as neighbors  from map_dataframe a , map_dataframe b where (a.x = b.x +1 or a.x = b.x or a.x = b.x -1 ) and (a.y = b.y +1 or a.y = b.y or a.y = b.y -1 ) and (a.z = b.z +1 or a.z = b.z or a.z = b.z -1 ) group by  a.x, a.y, a.z  ").persist()
  neighbor_dataframe.createOrReplaceTempView("neighbor_dataframe")
  


  var getis_score = spark.sql("select x,y,z , get_getis_score(weight, "+ mean +","+ standard_dev +" ,neighbors , "+ numCells +" )  as getis_score from neighbor_dataframe ").persist()
  

  getis_score.createOrReplaceTempView("getisscore_dataframe")


  var result_dataframe = spark.sql("select x,y,z  from getisscore_dataframe where getis_score not like 'NaN' order by getis_score desc")
  result_dataframe.show()
 
return result_dataframe


}
}
