import org.apache.spark.graphx.{Graph=>G, VertexId, Edge}
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object GraphComponents {
	def main ( args: Array[String] ){
		val conf=new SparkConf().setAppName("Pregel Graph")
		val sc = new SparkContext(conf)
		val inputgg =sc.textFile(args(0)).map(line => {
			val (vert,adj)=line.
							split(",").splitAt(1) 
							(vert(0).toLong, 
							adj.toList.map(_.toLong))})

		val ie = cgraph(inputgg)                                                                 
		val newG: G[Long,Long]=GraphEdge(ie)
		var i=5;
		val cc=newG.pregel(Long.MaxValue,i)((id,oldgrp,newGroup)=> math.min(oldgrp,newGroup),
			triplet=>{
				if(triplet.attr<triplet.dstAttr){
				  Iterator((triplet.dstId,triplet.attr))
				}else if((triplet.srcAttr<triplet.attr)){
				  Iterator((triplet.dstId,triplet.srcAttr))
				}else{
				  Iterator.empty
				}
			},
			(c1,c2)=>math.min(c1,c2))
			val res = cc.vertices
			val count= res.map(newG=>(newG._2,1))
			val  finalvert=count.reduceByKey(_ + _).sortByKey()
			display(finalvert)  
		}

	def cgraph(reducedgraph:RDD[(Long,List[Long])]):RDD[Edge[Long]]={
        val ipGraph=reducedgraph.flatMap(a=> a._2.map(b=>(a._1,b)))
		val edgeRDD=ipGraph.map(node=>Edge(node._1,node._2,node._1))
		return edgeRDD
    }

	def GraphEdge(Edges:RDD[Edge[Long]]):G[VertexId,Long]={   
		val intgph= G.fromEdges(Edges,"defaultProperty").mapVertices((id,_)=>id)
		return intgph
	}

	def display(preggrap:RDD[(Long, Int)] ){
		val finalgraph=preggrap.map(keycom=>keycom._1.toString+" "+keycom._2.toString )
        finalgraph.collect().foreach(println)
	}
}