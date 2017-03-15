package observatory

object Main extends App {
  val a = Extraction.locateTemperatures(2000, "/stations.csv", "/2000.csv")
  val b = Extraction.locationYearlyAverageRecords(a)
  println("hi")
}
