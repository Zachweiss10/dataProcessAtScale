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
    // Load the original data from a data source
    var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
    pickupInfo.createOrReplaceTempView("nyctaxitrips")
    pickupInfo.show()
    // Assign cell coordinates based on pickup points
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
  //  pickupInfo.show()
    pickupInfo.createOrReplaceTempView("pickupData")
    // Define the min and max of x, y, z
    val minX = -74.50/HotcellUtils.coordinateStep
    val maxX = -73.70/HotcellUtils.coordinateStep
    val minY = 40.50/HotcellUtils.coordinateStep
    val maxY = 40.90/HotcellUtils.coordinateStep
    val minZ = 1
    val maxZ = 31
    val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)
    pickupInfo = spark.sql("SELECT x, y, z FROM pickupData WHERE x >= " + minX + " and x <= "
      + maxX + " and y >= " + minY + " and y <= " + maxY + " and z >= " + minZ + " and z <= " + maxZ)
    pickupInfo.createOrReplaceTempView("boundedCells")
  //  pickupInfo.show()
    val countByCell = spark.sql("SELECT x, y, z, COUNT(*) AS ptCount FROM boundedCells GROUP BY x, y, z")
    countByCell.createOrReplaceTempView("countByCell")
//    countByCell.show()
    val mean = pickupInfo.count() / numCells
  //  println("mean:" + mean)
    val sumAttr = spark.sql("SELECT SUM(ptCount * ptCount) from countByCell").first().getLong(0).toDouble
  //  println("sumAttr: " + sumAttr)
    val std = Math.sqrt((sumAttr / numCells) - Math.pow(mean, 2))
  //  println("std: " + std)
    spark.udf.register("countNeighbours", (x: Int, y: Int, z: Int, minX: Int, maxX: Int, minY: Int, maxY: Int, minZ: Int, maxZ: Int) => ((HotcellUtils.countNeighbours(x, y, z, minX, minY, minZ, maxX, maxY, maxZ))))
    val joinNeighbors = spark.sql("SELECT t1.x, t1.y, t1.z, SUM(t2.ptCount) AS SumCount, " +
      "countNeighbours(t1.x, t1.y, t1.z, " + minX + "," + maxX + "," + minY + "," + maxY + "," + minZ + "," + maxZ + ") AS NeighCount " +
      "FROM countByCell t1 " +
      "CROSS JOIN countByCell t2 " +
      "WHERE ABS(t2.x - t1.x) <= 1 AND ABS(t2.y - t1.y) <= 1 AND ABS(t2.z - t1.z) <= 1 " +
      "GROUP BY t1.x, t1.y, t1.z")
    joinNeighbors.createOrReplaceTempView(("joinNeighbors"))
//    println("crossjoin:")
//    joinNeighbors.show()
    spark.udf.register("calcGetisOrd", (neighCount: Double, sumCount: Double, numCells: Int, mean: Double, stDev: Double) => ((HotcellUtils.calcGetisOrd(neighCount, sumCount, numCells, mean, stDev))))
    val getisOrdStat = spark.sql("select calcGetisOrd(NeighCount, SumCount, "+ numCells + ", " + mean + ", " + std + ") as zScore, x, y, z from joinNeighbors");
    getisOrdStat.createOrReplaceTempView("zScoreOutput")
//    getisOrdStat.show()
    val result = spark.sql("select x, y, z from zScoreOutput order by zScore desc, x desc, y asc, z desc")
    result.createOrReplaceTempView("result")
//    result.show()
    return result
  }
}
