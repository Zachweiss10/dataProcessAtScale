package cse512

import org.apache.spark.sql.SparkSession

object SpatialQuery extends App{
  def area(x1: Double, y1: Double, x2: Double, y2: Double, x3: Double, y3: Double) : Double =
        {
        return ((x1 * (y2 - y3) + x2 * (y3 - y1) + x3 * (y1 - y2)) / 2.0).abs;
    }
  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {
     
    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains", (queryRectangle:String, pointString:String) => {
      //      print("rectangle: ")
      //      print(queryRectangle)
      val point = pointString.split(",")
      //      print(" Point1:" + point(0))
      //      print(" Point2: " + point(1))
      val pointX = point(0).toDouble
      val pointY = point(1).toDouble
      val recPoints = queryRectangle.split(",")
      val botLX = recPoints(0).toDouble
      val botLY = recPoints(1).toDouble
      val topRX = recPoints(2).toDouble
      val topRY = recPoints(3).toDouble
      val botRX = topRX
      val botRY = botLY
      val topLX = botLX
      val topLY = topRY
      val rectArea = area(topLX, topLY, topRX, topRY, botRX, botRY) + area(topLX, topLY, botLX, botLY, botRX, botRY)
      //      print("rectArea: " + rectArea)
      val A1 = area(pointX, pointY, topLX, topLY, topRX, topRY)
      val A2 = area(pointX, pointY, topRX, topRY, botRX, botRY)
      val A3 = area(pointX, pointY, botRX, botRY, botLX, botLY)
      val A4 = area(pointX, pointY, botLX, botLY, topLX, topLY)
      val pointArea = A1 + A2 + A3 + A4
      //      println(", pointArea: " + pointArea)
      if ((pointArea - rectArea) <= .0000000001 ){
//                print("rectangle: ")
//                print(queryRectangle)
//                print(", rectArea: " + rectArea + "pointArea: " + pointArea)
//                println(" Exists in rectangle: " + pointX + ", " + pointY)
        true
      }
      else{
        false
      }
    })

    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains", (queryRectangle:String, pointString:String) => {
      //      print("rectangle: ")
      //      print(queryRectangle)
      val point = pointString.split(",")
      //      print(" Point1:" + point(0))
      //      print(" Point2: " + point(1))
      val pointX = point(0).toDouble
      val pointY = point(1).toDouble
      val recPoints = queryRectangle.split(",")
      val botLX = recPoints(0).toDouble
      val botLY = recPoints(1).toDouble
      val topRX = recPoints(2).toDouble
      val topRY = recPoints(3).toDouble
      val botRX = topRX
      val botRY = botLY
      val topLX = botLX
      val topLY = topRY
      val rectArea = area(topLX, topLY, topRX, topRY, botRX, botRY) + area(topLX, topLY, botLX, botLY, botRX, botRY)
      //      print("rectArea: " + rectArea)
      val A1 = area(pointX, pointY, topLX, topLY, topRX, topRY)
      val A2 = area(pointX, pointY, topRX, topRY, botRX, botRY)
      val A3 = area(pointX, pointY, botRX, botRY, botLX, botLY)
      val A4 = area(pointX, pointY, botLX, botLY, topLX, topLY)
      val pointArea = A1 + A2 + A3 + A4
      //      println(", pointArea: " + pointArea)
      if ((pointArea - rectArea) <= .0000000001 ){
//                print("rectangle: ")
//                print(queryRectangle)
//                print(", rectArea: " + rectArea + "pointArea: " + pointArea)
//                println(" Exists in rectangle: " + pointX + ", " + pointY)
        true
      }
      else{
        false
      }
    })

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>{
      val point1 = pointString1.split(",")
      val point2 = pointString2.split(",")
      val ptDist = math.sqrt(math.pow((point2(0).toDouble - point1(0).toDouble), 2) + math.pow((point2(1).toDouble - point1(1).toDouble), 2))
      if(ptDist <= distance){
//        print("baseDist: " + distance)
//        print(" DistBetweenPts: " + ptDist + "\n")
        true
      }
      else{
        false
      }
    })

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>{
      val point1 = pointString1.split(",")
      val point2 = pointString2.split(",")
      val ptDist = math.sqrt(math.pow((point2(0).toDouble - point1(0).toDouble), 2) + math.pow((point2(1).toDouble - point1(1).toDouble), 2))
      if(ptDist <= distance){
//        print("baseDist: " + distance)
//        print(" DistBetweenPts: " + ptDist + "\n")
        true
      }
      else{
        false
      }
    })
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

    return resultDf.count()
  }
}
