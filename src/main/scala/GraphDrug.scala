/**
 * Created by Lema Chowdary on 12/21/2015.
 */

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.{SparkContext, SparkConf}


object GraphDrug {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "F:\\winutils")
    val sconf=new SparkConf().setAppName("graphRDFGeneration").setMaster("local")
    val sc=new SparkContext(sconf)
    val drugBankRDD =sc.textFile("src/main/resources/DrugBank.nt")
    val arrayRDD= drugBankRDD.map(line=> line.split(" "))
    //val triple=arrayRDD.map(line=>"subject :"+ line(0)+"  predicate :"+line(1)+"  object :"+ line(2)).collect.foreach(println)
    val triples=arrayRDD.map(line=>(line(0),(line(1),line(2))))
    val subjects=arrayRDD.map(line=> line(0))
    val predicate=arrayRDD.map(line=>line(1))
    val objects=arrayRDD.map(line=>line(2))


    /** count and display of all subjects in inout file
      *
      */
    var sCount= subjects.count()
    println("subjects Count"+ sCount)
    //subjects.collect.foreach(println)


    /** count and all distinct subjects in input file/ RDD
      *
      */
    var sDistinctCount=subjects.distinct().count()
    println("DistinctCount of Subjects"+sDistinctCount)
    //subjects.distinct().collect.foreach(println)

    /**union of subjects and objects for assigning IDs
      *
      */
    val unionRDD =subjects.union(objects).distinct()
    //unionRDD.collect.foreach(println)

    /**assigning IDs to each distinct concept
     *
     */

    val conceptWithIdRDD=unionRDD.zipWithIndex().map({case(xyz,id)=>(xyz,id)})
    //conceptWithIdRDD.collect.foreach(println)
    val vertexRDD=conceptWithIdRDD.map(line=>(line._2,line._1))

    vertexRDD.coalesce(1,true).saveAsTextFile("src/main/resources/subjects")
    val mapper=conceptWithIdRDD.join(triples)
    val reorderedRDD=mapper.map(line=>(line._2._2._2,(line._2._1,line._2._2._1)))
    val edgeRDD=reorderedRDD.join(conceptWithIdRDD).map(line=>Edge(line._2._1._1,line._2._2,line._2._1._2))
    val graph=Graph(vertexRDD,edgeRDD)
    graph.edges.collect.foreach(println)
    //graph.vertices.collect.foreach(println)
  }

}
