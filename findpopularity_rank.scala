package com.bhattji.spark

import org.apache.spark.SparkContext
import org.apache.log4j._


object findpopularity_rank {
  
  /* This Function will split the data given in the "Marvel-names" file on the basis of '"' */
  
  def parsename(lines : String) : Option[(Int, String)] ={
    
    val fields = lines.split('\"')
    
    if(fields.length>1){
   
      return Some(fields(0).trim().toInt, fields(1).split('/')(0))                                                //field(1) is being split by / according to output required 
    }
    else{
      return None;
    }
  }
  
  /* Find the total number of affiliations of a superhero with other heros from "Marvel-graph" */
  
  def parseaffiliation(lines : String) = {
     
    val fields = lines.split("\\s+")
    
    (fields(0).toInt, fields.length-1)                                                                           // _1 element is of asked superhero and _2 element is number of total affiliations
  }
  
  // Main Function
  
  def main(Args : Array[String]){
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val sc = new SparkContext("local[*]","findpopularuty_rank")
    
    val HeroName_file = sc.textFile("../Marvel-names.txt")
    
    val NameRDD = HeroName_file.flatMap(parsename)                                                              // used defined function to split the data
   
    val reverse_NameRDD = NameRDD.map(x => (x._2,x._1))                                                        // swap 
    
    val total_superheros = NameRDD.count() + 1
    
    reverse_NameRDD.cache()                                                                                    // Cache the data for repeated usage
     
    //println("done")

    println()
    
    val input_Superhero = readLine("Enter SuperHero Name (Make Sure to Spell right): ").toUpperCase()
    
    println(s"Entered SuperHero : $input_Superhero")
    
    val Hero_idString = reverse_NameRDD.lookup(input_Superhero).mkString                                       // Look for the id of given superhero

    val Hero_id = Hero_idString.toInt                                                                          // Convert the id in integer format from wrapped array format
    
    //println(s"id of the superhero is : $Hero_id")
    
    val Hero_Affiliations = sc.textFile("../Marvel-graph.txt")
    
    val AffiliationRDD = Hero_Affiliations.map(parseaffiliation)
    
    val max_value = Hero_Affiliations.map(parseaffiliation).map(x => (x._2,x._1)).sortByKey().max()._1        // swap the map and sort it by the key(HeroID), find the maximum value of the key and grab the 1st element from the map  
    
    val friends = AffiliationRDD.lookup(Hero_id)                                                              //Look for previously find heroid and fetch affiliation total            
    
    println()
    
    //val number_of_appearance = friends.toInt
    if(friends.length == 0){
      println(s"id of the superhero is : $Hero_id \nNo affiliations found")
    }
    else{  
      
      val current_hero_affiliation_no = friends(0) 
      
      val popularity_percentage = 100 - (((max_value.toFloat - current_hero_affiliation_no.toFloat)/max_value.toFloat)*100)
      
      val popularity_rank = (total_superheros * (100 - popularity_percentage))/100
      
      println(s"id of the superhero is : $Hero_id \nNumber of affiliations: " +  friends(0) + "\nPopularity of the superhero based on affiliation is: " + popularity_percentage + "%" + "\nPopularity rank of Superhero is : " + popularity_rank.toInt)
    } 
   
    val result = AffiliationRDD.map(x => (x._2,x._1)).sortByKey()
    
    // result.foreach(println)
    
    /*
     * Top 5 Results to understand
     * 
     * 1.Captain America
     * 2.Karnak [Inhuman]
     * 3.Abomination
     * 4.Captain Marvel
     * 5.Punisher
     *    
     */
    
  }
  
}