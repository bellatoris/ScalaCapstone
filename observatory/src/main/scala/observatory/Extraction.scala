package observatory

import java.io.File
import java.time.LocalDate

import org.apache.spark.{RangePartitioner, SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

/**
  * 1st milestone: data extraction
  */
object Extraction {

  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("observatory")
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  private def filePath(path: String) = {
    new File(this.getClass.getClassLoader.getResource("." + path).toURI).getPath
  }

  /**
    * filter and map
    * @param lines
    * @return
    */
  def rawStation(lines: RDD[String]) =
    lines.filter(line => {
      val arr = line.split(",")
      arr.size == 4 && arr(2) != "" && arr(3) != ""
    }).map(line => {
      val arr = line.split(",")
      val stn = if (arr(0) == "") -1 else arr(0).toInt
      val wban = if (arr(1) == "") -1 else arr(1).toInt
      ((stn , wban) , Location(lat = arr(2).toDouble,
                               lon = arr(3).toDouble))
    })

  def rawTemperature(lines: RDD[String], year: Int) =
    lines.filter(line => {
      val arr = line.split(",")
      arr(2) != "" && arr(3) != "" && arr(4).toDouble != 9999.9
    }).map(line => {
      val arr = line.split(",")
      val stn = if (arr(0) == "") -1 else arr(0).toInt
      val wban = if (arr(1) == "") -1 else arr(1).toInt
      val month = arr(2).toInt
      val day = arr(3).toInt
      val temperature = arr(4).toDouble
      ((stn , wban) , (LocalDate.of(year, month, day), (temperature - 32) / 1.8))
    })

  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
  def locateTemperatures(year: Int, stationsFile: String, temperaturesFile: String): Iterable[(LocalDate, Location, Double)] = {
    val temperature = sc.textFile(filePath(temperaturesFile))
    val station = sc.textFile(filePath(stationsFile))
    val rawS = rawStation(station)
    val rawT = rawTemperature(temperature, year)

    // need to ordering by date
    rawT.join(rawS).map(x => (x._2._1._1, x._2._2, x._2._1._2)).collect()
  }

  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Double)]): Iterable[(Location, Double)] = {
    records.groupBy(x => x._2).mapValues(x => x.aggregate(0.0)((x, y) => x + y._3, _ + _ ) / x.size)
  }

}
