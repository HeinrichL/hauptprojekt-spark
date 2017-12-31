import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.graphx.lib.LabelPropagation
import java.util.Calendar

object Test {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("GraphX TSP Ant Colony Optimization")
              .setMaster("spark://localhost:7077")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    
    val start = Calendar.getInstance().getTime()
    println(start)
    
    val replygraph = GraphLoader.edgeListFile(sc, "input/higgs-reply_network.edgelist")//.cache()      39k
    val mentiongraph = GraphLoader.edgeListFile(sc, "input/higgs-mention_network.edgelist")//.cache()  116k
    val retweetgraph = GraphLoader.edgeListFile(sc, "input/higgs-retweet_network.edgelist")//.cache()  256k
    val socialgraph = GraphLoader.edgeListFile(sc, "input/higgs-social_network.edgelist")//.cache()    456k
            
    val tri = LabelPropagation.run(replygraph, 20)
    //val tri = socialgraph.triangleCount()
    println(tri.vertices.count())
    
    val end = Calendar.getInstance().getTime()
    println(end)
  }
}