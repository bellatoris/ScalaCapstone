package observatory

object Main extends App {
  val a = Extraction.locateTemperatures(2000, "/stations.csv", "/2000.csv")
  val temperature = Extraction.locationYearlyAverageRecords(a)
  val color = List((60.0, Color(255, 255, 255)), (32.0, Color(255, 0, 0)), (12.0, Color(255, 255, 0)),
    (0.0, Color(0, 255, 255)), (-15.0, Color(0, 0, 255)), (-27.0, Color(255, 0, 255)),
    (-50.0, Color(33, 0, 107)), (-60.0, Color(0, 0, 0)))
  val image = Visualization.visualize(temperature, color)
  image.output(new java.io.File("target/some-image.png"))
}
