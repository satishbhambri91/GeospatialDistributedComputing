package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {

  	

  	var xmin:Double = 0.0
  	var xmax:Double = 0.0
  	var ymin:Double = 0.0
  	var ymax:Double = 0.0

  	var point = pointString.split(",")
  	var pointLatitude:Double = point(0).toDouble
  	var pointLongitude:Double = point(1).toDouble

  	var rectangle = queryRectangle.split(",")
  	var x1:Double = rectangle(0).toDouble
  	var y1:Double = rectangle(1).toDouble
  	var x2:Double = rectangle(2).toDouble
  	var y2:Double = rectangle(3).toDouble


  	 if (x1>x2){
      xmin = x2
      xmax = x1
    }
 	else{
      xmin = x1
      xmax = x2
    }
    
    if (y1 > y2){
      ymin = y2
      ymax = y1
    }
    else{
      ymin = y1
      ymax = y2
    }

    if (((pointLatitude >= xmin) && (pointLatitude <= xmax )) && ((pointLongitude >=ymin) && (pointLongitude<=ymax)))
    {
      true
    }
    
     
      else false
    }

 
    
  }

  
