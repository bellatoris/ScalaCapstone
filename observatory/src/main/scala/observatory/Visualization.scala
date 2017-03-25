package observatory

import com.sksamuel.scrimage.{Image, Pixel}
import scala.math._


/**
  * 2nd milestone: basic visualization
  */
object Visualization {

  /**
    * Guess the temperature by using Inverse distance weighting
    * Get distance by using Great-circle distance
    * @param temperatures Known temperatures: pairs containing a location and the temperature at this location
    * @param location Location where to predict the temperature
    * @return The predicted temperature at `location`
    */
  def predictTemperature(temperatures: Iterable[(Location, Double)], location: Location): Double = {
    def deltaSigma(location1: Location, location2: Location): Double = {
      val loc1 = Location(location1.lat * Pi / 180, location1.lon * Pi / 180)
      val loc2 = Location(location2.lat * Pi / 180, location2.lon * Pi / 180)

      val sigma = acos(sin(loc1.lat) * sin(loc2.lat) + cos(loc1.lat) * cos(loc2.lat) * cos(loc1.lon - loc2.lon))
      sigma
    }
    // (distance, temperature)
    val distance = temperatures.par.map(pair => (deltaSigma(pair._1, location), pair._2))

    // check whether there is same location with target location in data.
    val zeroDistance = distance.filter(pair => pair._1 == 0)

    // power of inverse weight
    val power = -3

    if (zeroDistance.isEmpty) {
      val weightedSum = distance.aggregate(0.0)((sum, pair) => sum + pow(pair._1, -power) * pair._2, _ + _)
      val SumOfInverseWeight = distance.aggregate(0.0)((sum, pair) => sum + pow(pair._1, -power), _ + _)
      weightedSum / SumOfInverseWeight
    } else zeroDistance.head._2
  }

  /**
    * @param points Pairs containing a value and its associated color
    * @param value The value to interpolate
    * @return The color that corresponds to `value`, according to the color scale defined by `points`
    */
  def interpolateColor(points: Iterable[(Double, Color)], value: Double): Color = {
    val sortedPoints = points.toArray.sortBy(pair => pair._1)

    if (value < sortedPoints.head._1) {
      sortedPoints.head._2
    } else if (sortedPoints.last._1 < value) {
      sortedPoints.last._2
    } else if (sortedPoints.exists(point => point._1 == value)) {
      sortedPoints.dropWhile(point => point._1 != value).head._2
    } else {
      def interpolate(x0: Double, x1: Double, y0: Int, y1: Int, x: Double): Int =
        round(y0 + (x - x0) * (y1 - y0) / (x1 - x0)).toInt
      val index = sortedPoints.indexWhere(point => value < point._1)
      val closest1 = sortedPoints(index-1)
      val closest2= sortedPoints(index)

      Color(interpolate(closest1._1, closest2._1, closest1._2.red, closest2._2.red, value),
        interpolate(closest1._1, closest2._1, closest1._2.green, closest2._2.green, value),
        interpolate(closest1._1, closest2._1, closest1._2.blue, closest2._2.blue, value))
    }
  }

  /**
    * @param temperatures Known temperatures
    * @param colors Color scale
    * @return A 360×180 image where each pixel shows the predicted temperature at its location
    */
  def visualize(temperatures: Iterable[(Location, Double)], colors: Iterable[(Double, Color)]): Image = {
    val img =
      (0 until 360).par.flatMap(x => (0 until 180).map(y => {
        val color= interpolateColor(colors, predictTemperature(temperatures, Location(x - 180, -y + 90)))
        val alpha = 1
        Pixel(color.red, color.green, color.blue, alpha)}))
    Image(360, 180, img.toArray[Pixel])
  }
}

