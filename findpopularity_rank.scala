package com.bhattji.spark

import org.apache.spark.SparkContext
import org.apache.log4j._


object findpopularity_rank {
  
 
  def parsename(lines : String) : Option[(Int, String)] ={
    
    val fields = lines.split('\"')
    
    if(fields.length>1){
   
      return Some(fields(0).trim().toInt, fields(1))
    }
    else{
      return None;
    }
  }
  
  // for further expansion
  def findhero(lines : String, Heroname : String) : Option[(Int, String)] = {
    
    val fields = lines.split('\"')
    
    if(fields(1) == Heroname){
      
      return Some(fields(0).trim().toInt, fields(1))
    }
    else{
      return None;
    }
    
  }
  
  
  def parseaffiliation(lines : String) = {
     
    val fields = lines.split(' ')
    
    (fields(0), fields.length-1)
  }
  
  def main(Args : Array[String]){
    
    val input_Superhero = readLine("Enter SuperHero Name: ").toUpperCase()
    
    println(s"Entered SuperHero : $input_Superhero")
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val sc = new SparkContext("local[*]","findpopularuty_rank")
    
    val HeroName_file = sc.textFile("../Marvel-names.txt")
    
    val NameRDD = HeroName_file.flatMap(parsename)
    
    NameRDD.cache()
    
    val find_id_1 = NameRDD.filter(x => 
      {
        
        val name = x._1
        val id = x._2
        
        (name == input_Superhero)
        
      }).collect().foreach(println)
   
    
   
    
    //val matchHero = NameRDD.
    
    //for(data <- matchHero){if(data == input_Superhero) println("true")}
    
    //for(name <- matchHero) if(name == input_Superhero){println("found")}
    
   // val Hero_Affiliations = sc.textFile("../Marvel-graph.txt")
    
   // val AffiliationRDD = Hero_Affiliations.map(parseaffiliation).map(x => (x._2,x._1)).sortByKey()
    
    //val result = AffiliationRDD.collect()
    
    println("done")
    
    //NameRDD.foreach(println)
    
    
  }
  
}