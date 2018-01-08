import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.EdgeRDD


object MultiGraph {
  def main(args: Array[String]){
    val conf = new SparkConf()
      .setAppName("MultiGraph")
      .setMaster("local[*]")
      
    val sc = new SparkContext(conf)

    // load all layers from higgs twitter graph
    val graphSocial: Graph[Int, String] =  GraphLoader
      .edgeListFile(sc, "C:\\Users\\Heinrich\\Desktop\\Projekte\\Uni\\Hauptprojekt\\Daten\\Higgs Twitter\\higgs-social_network.edgelist")
      .mapEdges(e => "SOCIAL")
      .cache()
      
    val graphRetweet: Graph[Int, String] =  GraphLoader
      .edgeListFile(sc, "C:\\Users\\Heinrich\\Desktop\\Projekte\\Uni\\Hauptprojekt\\Daten\\Higgs Twitter\\higgs-retweet_network.edgelist")
      .mapEdges(e => "RETWEET")
      .cache()
      
    val graphMention: Graph[Int, String] =  GraphLoader
      .edgeListFile(sc, "C:\\Users\\Heinrich\\Desktop\\Projekte\\Uni\\Hauptprojekt\\Daten\\Higgs Twitter\\higgs-mention_network.edgelist")
      .mapEdges(e => "MENTION")
      .cache()
      
    val graphReply: Graph[Int, String] =  GraphLoader
      .edgeListFile(sc, "C:\\Users\\Heinrich\\Desktop\\Projekte\\Uni\\Hauptprojekt\\Daten\\Higgs Twitter\\higgs-reply_network.edgelist")
      .mapEdges(e => "REPLY")
      .cache()
      
    // merge all the edges to one RDD
    val unionEdges = graphSocial.edges
                        .union(graphRetweet.edges)
                        .union(graphMention.edges)
                        .union(graphReply.edges)
                                  
    // create multi layer graph
    val merged = Graph.fromEdges(unionEdges, "ha")
    
    merged.edges.groupBy(e => e.attr).collect().foreach(l => println(l._1 + "---" + l._2.count(e => true)))
  }
}