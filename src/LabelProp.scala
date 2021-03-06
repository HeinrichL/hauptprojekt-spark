import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.lib.LabelPropagation
import org.apache.spark.graphx.GraphLoader

object LabelProp {
  def main(args: Array[String]) {

    val file = Config.hdfs + args(0)
    val numIter = args(1).toInt

    val conf = new SparkConf()
      .setAppName("Label Propagation " + file)//.setMaster("local[*]")

    val sc = new SparkContext(conf)

    val graph: Graph[Int, Int] =  GraphLoader.edgeListFile(sc, file).cache()

    val computed = LabelPropagation.run(graph, numIter)
        
    println(computed.vertices.count())
  }
}