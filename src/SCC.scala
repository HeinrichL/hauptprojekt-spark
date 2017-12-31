import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.GraphLoader

object SCC {
  def main(args: Array[String]) {

    val file = args(0)
    val numIter = args(1).toInt

    val conf = new SparkConf().setAppName("SCC " + file)
    val sc = new SparkContext(conf)
    
    val graph: Graph[Int, Int] =  GraphLoader.edgeListFile(sc, "hdfs://hdfs-namenode-0.hdfs-namenode.abk609.svc.cluster.local/" + file).cache()

    val computed = graph.stronglyConnectedComponents(numIter)
        
    println(computed.vertices.count())
  }
}