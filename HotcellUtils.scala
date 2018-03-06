package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.spark.sql.DataFrame
import scala.collection.mutable.ListBuffer

object HotcellUtils 
{
  val coordinateStep = 0.01
  

  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int =
  {
    // Configuration variable:
    // Coordinate step is the size of each cell on x and y
    var result = 0
    coordinateOffset match
    {
      case 0 => result = Math.floor((inputString.split(",")(0).replace("(","").toDouble/coordinateStep)).toInt
      case 1 => result = Math.floor(inputString.split(",")(1).replace(")","").toDouble/coordinateStep).toInt
      // We only consider the data from 2009 to 2012 inclusively, 4 years in total. Week 0 Day 0 is 2009-01-01
      case 2 => {
        val timestamp = HotcellUtils.timestampParser(inputString)
        result = HotcellUtils.dayOfMonth(timestamp) // Assume every month has 31 days
      }
    }
    return result
  }

  def timestampParser (timestampString: String): Timestamp =
  {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val parsedDate = dateFormat.parse(timestampString)
    val timeStamp = new Timestamp(parsedDate.getTime)
    return timeStamp
  }

  def dayOfYear (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_YEAR)
  }

  def dayOfMonth (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_MONTH)
  }

  // YOU NEED TO CHANGE THIS PART
  def get_number_neighbours(x: Int,y:Int,z:Int,maxX:Int,maxY:Int,maxZ:Int,minX:Int,minY:Int,minZ:Int): List[String] =
  {
    var neighbours_list = ListBuffer.empty[String]
    var tempX = -1
    while (tempX < 2) {
      var tempY = -1
        while (tempY < 2) {
          var tempZ = -1
            while (tempZ < 2) {
                var latitude = x
                var longitude = y
                var day = z
                latitude += tempX
                longitude += tempY
                day += tempZ
                if (!((tempX == 0) && (tempY == 0) && (tempZ == 0))) 
                {
                    if (latitude >= (minX ) && latitude <= (maxX )) 
                      {
                        if (longitude <= (maxY ) && longitude >= (minY )) 
                        {
                          if(day >= minZ && day <= maxZ)
                          {
                            val neighbour_coordinate=latitude + "," + longitude +"," + day
                            neighbours_list += neighbour_coordinate
                         
                          }
                        }
                      }
                }
              tempZ += 1
            }
            tempY += 1
        }
        tempX += 1
        
    }
    return neighbours_list.toList
  }
  
  
  def get_getis_score(weight: Int,xbar: Double,std_dev: Double,count_neighbors: Int,count_cells: Int): Double = {

    //val numerator = weight - (xbar * count_neighbors)
    
    //val temp_denominator = (count_cells * count_neighbors) - math.pow(count_neighbors , 2)
    
    //val denominator =  std_dev * math.sqrt(((count_cells * count_neighbors) - math.pow(count_neighbors , 2)).toDouble / (count_cells  - 1 )))
    
    val getis_score = (weight - (xbar * count_neighbors)).toDouble / (std_dev * math.sqrt(((count_cells * count_neighbors) - math.pow(count_neighbors , 2)).toDouble / (count_cells  - 1 )))
    
    return getis_score
  }
}
