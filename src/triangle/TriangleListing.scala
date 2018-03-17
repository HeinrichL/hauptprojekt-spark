package triangle

import org.apache.spark.graphx._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.util.collection.OpenHashSet
import scala.collection.mutable.ListBuffer

object TriangleListing {

  type VertexSet = OpenHashSet[VertexId]

  def main(args: Array[String]): Unit = {
    val file = Config.hdfs + args(0)
    //val file = "C:/Users/Heinrich/Desktop/Projekte/Uni/Hauptprojekt/Flink/input/triangles.g" //graph_10000.edgelist

    val conf = new SparkConf().setAppName("Triangle Listing " + file)//.setMaster("local[*]")
    val sc = new SparkContext(conf)
    //Config.hdfs +
    val graph: Graph[Int, Int] = GraphLoader.edgeListFile(sc, file).cache()

    // Construct set representations of the neighborhoods
    val nbrSets: VertexRDD[VertexSet] =
      graph.collectNeighborIds(EdgeDirection.Either).mapValues { (vid, nbrs) =>
        val set = new VertexSet(nbrs.length)
        var i = 0
        while (i < nbrs.length) {
          // prevent self cycle
          if (nbrs(i) != vid) {
            set.add(nbrs(i))
          }
          i += 1
        }
        set
      }

    // join the sets with the graph
    val setGraph: Graph[VertexSet, Int] = graph.outerJoinVertices(nbrSets) {
      (vid, _, optSet) => optSet.getOrElse(null)
    } //.mapVertices((id, value) => value)

    val graphWithIntersectedNeighborSets = setGraph.mapTriplets(triplet => {
      val (smallSet, largeSet) = if (triplet.srcAttr.size < triplet.dstAttr.size) {
        (triplet.srcAttr, triplet.dstAttr)
      } else {
        (triplet.dstAttr, triplet.srcAttr)
      }

      val set = new VertexSet(smallSet.size)
      val iter = smallSet.iterator
      while (iter.hasNext) {
        val vid = iter.next()
        if (vid != triplet.srcId && vid != triplet.dstId && largeSet.contains(vid)) {
          set.add(vid)
        }
      }
      set
    })
    
    //println(graphWithIntersectedNeighborSets.edges.collect().mkString("\n"))

    val trianglesWithPermutations = graphWithIntersectedNeighborSets.edges.flatMap(e => {
      val iter = e.attr.iterator
      var res = new ListBuffer[(VertexId, VertexId, VertexId)]()
      while (iter.hasNext) {
        var v1 = iter.next()
        var v2 = e.srcId
        var v3 = e.dstId

        val min = Math.min(v1, Math.min(v2, v3))
        val max = Math.max(v1, Math.max(v2, v3))
        val middle = if(v1 == min) Math.min(v2,v3) else if (v1 == max) Math.max(v2, v3) else v1
        
        res += Tuple3(min, middle, max)
      }
      res
    })
    
//    val trianglesWithPermutations = graphWithIntersectedNeighborSets.edges.flatMap(e => {
//      List(e.attr)
//    })
    
    val triangles = trianglesWithPermutations.distinct()

    println(triangles.takeSample(false, 100000).mkString("\n"))
  }

  def swap(v1: VertexId, v2: VertexId): (VertexId, VertexId) = {
    (v2, v1)
  }
}
