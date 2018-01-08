import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.EdgeRDD
import org.apache.spark.graphx.util.GraphGenerators
import scala.util.Random


object MultiGraphGenerator {
  def main(args: Array[String]){
    val conf = new SparkConf()
      .setAppName("MultiGraph")
      .setMaster("local[*]")
      
    val sc = new SparkContext(conf)
    
    val layer = Seq(1,2,3,4)

    // load all layers from higgs twitter graph
    val g : Graph[Long, Int] = GraphGenerators.logNormalGraph(sc, 10)
      .mapEdges(e => {
        val r = new Random()
        Math.abs(r.nextInt() % 4 + 1)
      }).cache()
    
    println(g.edges.collect().mkString("\n"))
      
  }
}