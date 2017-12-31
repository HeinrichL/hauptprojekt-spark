import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import java.io.Writer
import java.io.PrintWriter
import java.io.File

object MyGraphGenerator {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("GraphX Graph Generator")
      .setMaster("local[1]")
    val sc = new SparkContext(conf)
    
    val vCount = 456000

    var i = 1
    //for(i <- 1 to 10){
      val graph: Graph[Long, Int] =
      GraphGenerators
        .logNormalGraph(sc, numVertices = vCount.toInt * i, seed = 1231231L).cache()
      
    graph.edges.saveAsTextFile("out/graph_" + vCount * i + ".edgelist")
    //val pw = new PrintWriter(new File("out/graph_" + vCount * i + ".edgelist"))
    //graph.edges.collect().foreach(e => pw.write(e.srcId + " " + e.dstId + "\n"))
    //pw.close
    //}
    
  }
}