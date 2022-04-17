import scala.math._
import scala.collection.parallel.CollectionConverters._
//import java.util.concurrent.atomic.AtomicInteger

package com.destinatusobdura {

case class TSPProblem (nodes : Int, dist : Array[Array[Int]]) {
  val nearestNeighborSol = nearestNeighbor

  private def nearestNeighbor : Seq[Int] = {
      var s = scala.collection.mutable.ListBuffer(0)
      while (s.length < nodes) {
        val curNode = s.last
        var best = -1
        var bestval = Int.MaxValue
        for (i <- Range(0, nodes).filter (x => ! s.contains (x))) {
          if (dist(curNode)(i) < bestval) {
            best = i
            bestval = dist(curNode)(i)
          }
        }
        s.append (best)
      }
      return s.toSeq
  }

  //val bestSol = new AtomicInteger (measureTSP (nearestNeighborSol, dist))

  def bnb (b : Int) : Seq[Int] = {
    var best = b
    val nodeSet = Range (0, nodes).toSet
    val mins = dist.map (_.filter(_ != 0).sorted.take(2).sum / 2)
    def bnbRec (start : Seq[Int]) : Option[(Seq[Int], Int)] = {
      val len = TSPProblem.measureTSP (start, dist)
      if (len >= best) return None
      else if (start.length == nodes) {
        System.out.println (start.toString + " : " + len.toString)
        best = len
        return Some(start, len)
      } else {
        val remainingNodes = (nodeSet -- start).toSeq
        val minLen = len + remainingNodes.map (x => mins (x)).sum
        val newNodes = remainingNodes.map (n => (n, dist(start.last)(n))).sortBy(_(1))
        val results = newNodes.par.map (n => bnbRec (start.appended(n(0)))).flatten
        if (results.isEmpty) None
        else Some (results.minBy (_(1)))
      }      
    }
    bnbRec (Seq(0)).get.apply(0)
  }
}
object TSPProblem {
  private def eucDist (a : (Int, Int), b : (Int, Int)) : Int = {
    val xdist = (a(0) - b(0)).toDouble
    val ydist = (a(1) - b(1)).toDouble

    val sqdist = xdist * xdist + ydist * ydist
    (scala.math.sqrt (sqdist)).toInt
  }

  private def geoDist (a : (Double, Double), b : (Double, Double)) : Int = {
    def toRad (d : Double) : Double = {
      val PI = 3.141592;
      val deg = d.toInt; // nint ?
      val min = d - deg;
      PI * (deg + 5.0 * min/ 3.0) / 180.0;
    }
    if (a == b) {
      0
    } else {
      val RRR = 6378.388;
      val q1 = cos( toRad(a(1)) - toRad(b(1)));
      val q2 = cos( toRad(a(0)) - toRad(b(0)));
      val q3 = cos( toRad(a(0)) + toRad(b(0)));
      ( RRR * acos( 0.5*((1.0+q1)*q2 - (1.0-q1)*q3) ) + 1.0).toInt
    }
  }

  def fromLocs (loc : Seq [(Int, Int)]) : TSPProblem = {
    val nodes = loc.length
    val dist : Array[Array[Int]] = Range (0, nodes).map (i => Range (0, nodes).map (
        j => eucDist (loc(i), loc(j))
      ).toArray).toArray
    TSPProblem (nodes, dist)
  }

  def fromGeo (loc : Seq [(Double, Double)]) : TSPProblem = {
    val nodes = loc.length
    val dist : Array[Array[Int]] = Range (0, nodes).map (i => Range (0, nodes).map (
        j => geoDist (loc(i), loc(j))
      ).toArray).toArray
    TSPProblem (nodes, dist)
  }

  def measureTSP (run : Seq[Int], dist : Array[Array[Int]], partial : Boolean = false) : Int = {
    if (run.length < 2) 0
    else run.sliding (2).map (s => dist (s(0))(s(1))).sum + (if (partial) 0 else dist (run.head) (run.last)) 
  }

  def fromGeoFile (fileName : String) : TSPProblem = {
    val bufferedSource = scala.io.Source.fromFile(fileName)
    var isGeo = false
    val locs = scala.collection.mutable.ArrayBuffer[(Double, Double)] ()
    var break = false
    for (lines <- bufferedSource.getLines()) {
      if (break) {}
      else if (lines == "EDGE_WEIGHT_TYPE: GEO") {
        isGeo = true
      }
      else if (lines.startsWith ("COMMENT: ") || lines.startsWith ("NAME:")) {
        System.out.println (lines)
      }
      else if (lines.contains ("EOF")) {
        break = true
      }
      else if (lines.startsWith (" ")) {
        val split = lines.strip.split ("  *")
        locs += ((split(1).toDouble, split(2).toDouble))
      }
    }
    bufferedSource.close()
    assert (isGeo)
    fromGeo (locs.toSeq)
  }
}

object TSP {
  val diamond = TSPProblem.fromLocs (Seq((0, 0), (50000, 1000), (50000, -1000), (100000, 0)))

  val burma14 = TSPProblem.fromGeo (Seq(
    (16.47,       96.10),
    (16.47,       94.44),
    (20.09,       92.54),
    (22.39,       93.37),
    (25.23,       97.24),
    (22.00,       96.05),
    (20.47,       97.02),
    (17.20,       96.29),
    (16.30,       97.38),
    (14.05,      98.12),
    (16.53,       97.38),
    (21.52,       95.59),
    (19.41,       97.13),
    (20.09,       94.55)
  ))

  val ulysses16 = TSPProblem.fromGeo (Seq(
 (38.24, 20.42),
 (39.57, 26.15),
 (40.56, 25.32),
 (36.26, 23.12),
 (33.48, 10.54),
 (37.56, 12.19),
 (38.42, 13.11),
 (37.52, 20.44),
(41.23 , 9.10),
 (41.17 ,13.05),
 (36.08, -5.21),
 (38.47, 15.13),
 (38.15, 15.35),
 (37.51, 15.17),
 (35.49, 14.32),
 (39.36, 19.56)
  ))



  def main(args: Array[String]) =  {
    val prob = TSPProblem.fromGeoFile (args(0))
    println (prob.dist.map (_.max).max)
    val nn = prob.nearestNeighborSol
    val nndist = TSPProblem.measureTSP(nn, prob.dist)

    System.out.println (nn.toString + " : " + nndist)

    val bnb = prob.bnb (nndist)
    System.out.println (bnb)
    System.out.println (TSPProblem.measureTSP(bnb, prob.dist))
  }
}
}