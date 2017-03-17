import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD



object SimpleApp {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val raw = sc.textFile("gs://jacondelw/every10.tsv")
    //val raw = sc.textFile("/home/jake/Desktop/textproc/subsets/subset.tsv")

    val edges: RDD[Edge[String]] =
      raw.map { line =>
        val fields = line.split(" ")
        Edge(fields(0).toLong, fields(1).toLong )
      }


    val graph = Graph.fromEdges(edges,0).mapEdges(_ => 1.toLong)

    val sourceId: VertexId = 1355139 // The ultimate source
    // Initialize the graph such that all vertices except the root have distance infinity.

    val initialGraph : Graph[(Long, List[VertexId]), Long] = graph.mapVertices((id, _) => if (id == sourceId) (0, List[VertexId](sourceId)) else (10000.toLong, List[VertexId]()))

    val sssp = initialGraph.pregel((10000.toLong, List[VertexId]()), 10 , EdgeDirection.Out)(

      // Vertex Program
      (id, dist, newDist) => if (dist._1 < newDist._1) dist else newDist, 

      // Send Message
      triplet => {
        val sourceLength = triplet.srcAttr._1
        val destLength = triplet.dstAttr._1
        val edgeLength = triplet.attr
        if ( sourceLength + edgeLength < destLength ) {
          val destId = triplet.dstId
          val sourcePath = triplet.srcAttr._2
          Iterator( (destId, (sourceLength + edgeLength , sourcePath :+ destId)) )
        } else {
          Iterator.empty
        }
      },
      //Merge Message
      (a, b) => if (a._1 < b._1) a else b)
     


    val res = sssp.vertices

    //val rescsv = res.filter( _._2._1 < 10000).map({
    //  case (id, (length,path)) => id+","+length+","+ path.mkString(",")
    //})

    val resF = res.filter(_._2._1 < 10000)
    val resFFlat = resF.map({ case (id,(length,path)) => (id,length,path)})

    //rescsv.saveAsTextFile("gs://jacondelw/every10result/")
    //resFFlat.toDF.write.parquet("gs://jacondelw/every10result/")
    sqlContext.createDataFrame(resFFlat).write.parquet("gs://jacondelw/every10resultpq/")
    sc.stop()
  }
}
