import scala.math._
import scala.collection.parallel.CollectionConverters._
import scala.annotation.tailrec
import java.util.concurrent.atomic.AtomicInteger

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

  val bestSol = new AtomicInteger (TSPProblem.measureTSP (nearestNeighborSol, dist))

  private def twoOptSwap (route : Seq[Int], i : Int, j : Int) : Seq[Int] = {
    val (head, tail) = route.splitAt (i)
    val (middle, end) = tail.splitAt (j - i)
    val ret = head ++: middle.reverse ++: end
    //assert (ret.length == route.length)
    ret
  }

  private def twoOptLength (route : Seq[Int], origLen : Int, i : Int, j : Int) : Int = {
    //println (i.toString + "/" + j.toString)
    assert (j - i > 2)
    // Old length - dist (i - 1, i) - dist (j - 1, j) + dist (i - 1, j - 1) + dist (i, j)
    val newLen = origLen - dist (route (i - 1))(route(i)) - dist (route (j - 1))(route (j)) 
      + dist (route (i - 1))(route (j - 1)) + dist (route (i))(route(j))
    //assert (newLen == TSPProblem.measureTSP (twoOptSwap(route, i, j), dist))
    newLen
  }

  @tailrec final def twoOpt (route : Seq[Int], oL : Int = -1, i : Int = 1, j : Int = 4) : Seq[Int] = {
    val origLen = if (oL > 0) oL else TSPProblem.measureTSP (route, dist)
    if (j >= route.length) twoOpt (route, origLen, i + 1, i + 4)
    else if (i >= route.length) route
    else {
      val twoLen = twoOptLength (route, origLen, i, j)
      if (twoLen < origLen) twoOpt (twoOptSwap (route, i, j), twoLen)
      else twoOpt (route, origLen, i, j + 1)
    }
  }

  def bnb (b : Int) : Seq[Int] = {
    if (b < bestSol.get()) {
        var continue = true
        var currBest = bestSol.get()
        while (continue && !bestSol.compareAndSet(currBest, b)) {
          currBest = bestSol.get()
          if (b >= currBest) continue = false
        }
    }
    val nodeSet = Range (0, nodes).toSet
    val mins = dist.map (_.filter(_ != 0).sorted.take(2).sum / 2)
    def bnbRec (start : Seq[Int]) : Option[(Seq[Int], Int)] = {
      // Since it includes the line for the last point to the start, it only works on
      // spaces obeying the triangle inequality
      val len = TSPProblem.measureTSP (start, dist)
      var currBest = bestSol.get()
      if (len >= currBest) return None
      else if (start.length == nodes) {
        while (!bestSol.compareAndSet(currBest, len)) {
          currBest = bestSol.get()
          if (len >= currBest) return None
        }
        System.out.println (start.toString + " : " + len.toString)
        return Some(start, len)
      } else {
        val remainingNodes = (nodeSet -- start).toSeq
        val minLen = Math.max (
          len,
          TSPProblem.measureTSP (start, dist, true /* partial */) + remainingNodes.map (x => mins (x)).sum
        )
        if (minLen >= currBest) None
        else {
          val newNodes = remainingNodes.map (n => (n, dist(start.last)(n))).sortBy(_(1))
          val results = newNodes.par.map (n => bnbRec (start.appended(n(0)))).flatten
          if (results.isEmpty) None
          else Some (results.minBy (_(1)))
        }
      }      
    }
    bnbRec (Seq(0)).map(_.apply(0)).getOrElse (nearestNeighborSol)
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
      else if (lines.startsWith (" ") || lines.startsWith ("0")) {
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

  def main(args: Array[String]) =  {
    val prob = TSPProblem.fromGeoFile (args(0))
    //println (prob)
    //println (prob.dist.toSeq.map (_.toSeq))
    println ("Longest distance: " + prob.dist.map (_.max).max.toString)

    val bestTwo = prob.twoOpt (Range(0, prob.nodes).toSeq)
    val bestTwodist = TSPProblem.measureTSP(bestTwo, prob.dist)

    System.out.println ("2-Opt of 0-14: ")
    System.out.println (bestTwo.toString + " : " + bestTwodist)

    val nn = prob.nearestNeighborSol
    val nndist = TSPProblem.measureTSP(nn, prob.dist)

    System.out.println ("Nearest neighbor: ")
    System.out.println (nn.toString + " : " + nndist)

    val two = prob.twoOpt (nn)
    val twodist = TSPProblem.measureTSP(two, prob.dist)

    System.out.println ("2-Opt of NN: ")
    System.out.println (two.toString + " : " + twodist)

    var minimum = Math.min (Math.min(nndist, twodist), bestTwodist)
    System.out.println ("Minimum: " + minimum.toString)
    System.out.println ("--")
    val bnb = prob.bnb (minimum)
    System.out.println ("Final solution: " + bnb.toString)
    System.out.println ("Final distance: " + TSPProblem.measureTSP(bnb, prob.dist).toString)
  }
}
}