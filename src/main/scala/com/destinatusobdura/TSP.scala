  // TSP Scala
  // Copyright (C) 2022  David Starner
  //
  // This program is free software: you can redistribute it and/or modify
  // it under the terms of the GNU General Public License as published by
  // the Free Software Foundation, either version 3 of the License, or
  // (at your option) any later version.
  //
  // This program is distributed in the hope that it will be useful,
  //  but WITHOUT ANY WARRANTY; without even the implied warranty of
  //  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  //  GNU General Public License for more details.
  //
  // You should have received a copy of the GNU General Public License
  // along with this program.  If not, see <http://www.gnu.org/licenses/>.


import scala.math._
import scala.collection.parallel.CollectionConverters._
import scala.annotation.tailrec
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.PriorityBlockingQueue

import org.ojalgo.optimisation.ExpressionsBasedModel

package com.destinatusobdura {

final case class TSPPartialSol (sol : Seq[Int], estTotal : Int) extends Comparable[TSPPartialSol] {
  def compareTo (t: TSPPartialSol) = estTotal.compareTo (t.estTotal)
  // def compareTo (t: TSPPartialSol) = (estTotal - sol.length / 3).compareTo ((t.estTotal - t.sol.length / 3))
} 


final class BnBRunner (val t : TSPProblem, currBest : Int, currBestSol : Seq[Int]) {
  val nodeSet = Range (0, t.nodes).toSet
  val cores = Runtime.getRuntime().availableProcessors()
  val best  = new AtomicInteger (currBest)
  private var bestSol : Seq[Int] = currBestSol
  val bnbQueue = PriorityBlockingQueue[TSPPartialSol] (1000)

  def updateBest (sol: Seq[Int], newBest : Int) : Int = {
    var cb = best.get()
    synchronized {
    var beenSet = false
    while (cb > newBest && ! beenSet) {
      beenSet = best.compareAndSet (cb, newBest)
      cb = best.get()
    }
    if beenSet then {
      bestSol = sol
    }
    }
    System.out.println (newBest.toString + " : " + sol.toString)
    cb
  }
  def start = {
    bestSol = currBestSol
    bnbQueue.offer (TSPPartialSol (Seq(0), 0)) // no need for real estimated value
    val threads : Array[Thread] = Array.range (1, cores).map(n => new Thread (new BnBThread(this, n.toString)))
    threads(0).start()
    while (bnbQueue.size < cores) {
      Thread.sleep (100)
    }
    threads.tail.map (_.start())
    threads.map (_.join())
    (best.get, bestSol)
  }

}

class BnBThread (runner : BnBRunner, id : String) extends Runnable {
  def run : Unit = {
    while (true) {
      val problem = runner.bnbQueue.poll()
      if problem == null then {
        Thread.sleep (1000)
        if runner.bnbQueue.peek() == null then {
          //System.out.println (id + " dying")
          return
        }
      }
      else {
        val currBest = runner.best.get()
        // System.out.println (problem.estTotal.toString + " : " + problem.sol.toString + " : " + id)
        if (currBest > problem.estTotal) {
          assert (problem.sol.length < runner.t.nodes)
          val remainingNodes = runner.nodeSet -- problem.sol
          if remainingNodes.size == 1 then {
            val sol = problem.sol :+ remainingNodes.head
            val len = TSPProblem.measureTSP (sol, runner.t.dist)
            if len < currBest then {
              runner.updateBest (sol, len) 
              // updateBest is synchronized and makes sure we aren't overwriting any other answer
            }
          }
          else {
            for (i <- remainingNodes) {
              val sol = problem.sol :+ i
              val estLen = runner.t.lPApproximation (sol)
              if estLen < problem.estTotal then {
                // Ojalgo is broken in that it makes more constrained problems cheaper than their less constrained predecessors
                // when solving IP problems. Check for this mistake.
                System.out.println ("ERR: " + problem.sol + " = " + problem.estTotal + " and " + sol + " = " + estLen)
              }

              val currBest = runner.best.get()             
              if estLen < currBest then {
                runner.bnbQueue.offer(TSPPartialSol (sol, estLen))
              }
            }
          }
        }
      }
    }

  }
}

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

  private def twoOptSwap (route : Vector[Int], i : Int, j : Int) : Vector[Int] = {
    val (head, tail) = route.splitAt (i)
    val (middle, end) = tail.splitAt (j - i)
    val ret = head ++: middle.reverse ++: end
    //assert (ret.length == route.length)
    ret
  }

  private def twoOptLength (route : Seq[Int], origLen : Int, i : Int, j : Int) : Int = {
    assert (j - i > 2)
    // Old length - dist (i - 1, i) - dist (j - 1, j) + dist (i - 1, j - 1) + dist (i, j)
    val newLen = origLen - dist (route (i - 1))(route(i)) - dist (route (j - 1))(route (j)) 
      + dist (route (i - 1))(route (j - 1)) + dist (route (i))(route(j))
    //assert (newLen == TSPProblem.measureTSP (twoOptSwap(route, i, j), dist))
    newLen
  }

  def twoOpt (route : Seq[Int]) = twoOptRec (route.toVector)

  @tailrec final def twoOptRec (route : Vector[Int], oL : Int = -1, i : Int = 1, j : Int = 4) : Seq[Int] = {
    val origLen = if (oL > 0) oL else TSPProblem.measureTSP (route, dist)
    if (j >= route.length) twoOptRec (route, origLen, i + 1, i + 4)
    else if (i >= route.length) route
    else {
      val twoLen = twoOptLength (route, origLen, i, j)
      if (twoLen < origLen) twoOptRec (twoOptSwap (route, i, j), twoLen)
      else twoOptRec (route, origLen, i, j + 1)
    }
  }

  def lPApproximation (run : Seq[Int]) : Int = {
    def canon (i : Int, j : Int) : (Int, Int) ={
      if (i > j) (j, i)
      else (i, j)
    }
    val model = new ExpressionsBasedModel()
    val variables = Range(0, nodes).
      flatMap (x => Range (0, nodes).map (y => (x, y))).
      filter (x => x(0) != x(1) && x == canon(x(0), x(1))).
      map (
        (x, y) => ((x, y), model.addVariable (f"v$x$y").binary().weight (dist (x)(y)))
        //(x, y) => ((x, y), model.addVariable (f"v$x$y").lower(0).upper(1).weight (dist (x)(y)))
      ).
      toMap
    for (i <- Range(0, nodes)) {
      val inout = model.addExpression (f"node$i").lower(2).upper(2)
      for (j <- Range(0, nodes)) {
        if i != j then {
          inout.set (variables (canon(i, j)), 1)
        }
      }
    }
    val runl = run.length
    if (runl >= 2) {
      for (i <- Range (0, runl - 1)) {
        val preset = model.addExpression (f"preset$i").lower(1).upper(1).set (variables (canon (run(i), run (i + 1))), 1)
      }
    }
    val result = model.minimise()
    result.getValue.asInstanceOf[Int]
  }

}

object TSPProblem {
  private def eucDist (a : (Double, Double), b : (Double, Double)) : Int = {
    val xdist = (a(0) - b(0)).toDouble
    val ydist = (a(1) - b(1)).toDouble

    val sqdist = xdist * xdist + ydist * ydist
    (sqrt (sqdist)).toInt
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

  def fromEuc2D (loc : Seq [(Double, Double)]) : TSPProblem = {
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

  def fromFile (fileName : String) : TSPProblem = {
    val bufferedSource = scala.io.Source.fromFile(fileName)
    var isGeo = false
    var isEuc2D = false
    val locs = scala.collection.mutable.ArrayBuffer[(String, String)] ()
    var break = false
    for (lines <- bufferedSource.getLines()) {
      if (break) {}
      else if (lines == "EDGE_WEIGHT_TYPE: GEO") {
        isGeo = true
      }
      else if (lines == "EDGE_WEIGHT_TYPE: EUC_2D" || lines == "EDGE_WEIGHT_TYPE : EUC_2D") {
        isEuc2D = true
      }
      else if (lines.startsWith ("COMMENT: ") || lines.startsWith ("NAME:")) {
        System.out.println (lines)
      }
      else if (lines.contains ("EOF")) {
        break = true
      }
      else if (lines.startsWith (" ") || lines(0).isDigit) {
        val split = lines.strip.split ("  *")
        locs += ((split(1), split(2)))
      }
    }
    bufferedSource.close()
    val processedLocs = locs.toSeq.map ((x,y) => (x.toDouble, y.toDouble))
    if isGeo then
      fromGeo (processedLocs)
    else if isEuc2D then
      fromEuc2D (processedLocs)
    else
      throw IllegalArgumentException();
  }
}

object TSP {
  val diamond = TSPProblem.fromEuc2D (Seq((0, 0), (50000, 1000), (50000, -1000), (100000, 0)))

  def main(args: Array[String]) =  {
    val prob = TSPProblem.fromFile (args(0))

    println ("Longest distance: " + prob.dist.map (_.max).max.toString)

    println ("LP lower bound:" + prob.lPApproximation (Seq()).toString)

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

    val bnbRunner = BnBRunner (prob, Math.min (twodist, bestTwodist), 
      if bestTwodist < twodist then bestTwo else two)
    val bnb = bnbRunner.start

    System.out.println ("Final solution: " + bnb(1).toString)
    System.out.println ("Final distance: " + bnb(0).toString)
  }
}
}