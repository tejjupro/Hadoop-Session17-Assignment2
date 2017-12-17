/*
 * Average score per student_name across all grades is same as average score per
   student_name per grade
 */ 
  package studentAnalysis
 
  import org.apache.spark.SparkConf

import org.apache.spark.SparkContext
import org.apache.spark.sql.execution.Intersect

object StudentAnalaysis_3 {
  def main(args: Array[String]): Unit = {
    
  
      //creating an instance of SparkConf to provide the spark configurations.This will make spark to run in local mode
    val conf = new SparkConf().setAppName("Student Analaysis").setMaster("local")

    //Providing configuration parameter to SparkContext with an  instance of SparkConf
    val sc = new SparkContext(conf)

    
    val data = sc.textFile("/home/acadgild/scala_eclipse/studentDataset").map(line => line.split(",")).map(userRecord => (userRecord(0),
  userRecord(1), userRecord(2),userRecord(3).toInt,userRecord(4).toInt))
  
  
   //get the name and grade as key and total marks as value using map rdd
  val nameGradeRDD = data.map{case(name,subject,grade,marks1,marks2) =>  ((name,grade), (marks1.toInt + marks2.toInt))}
  
  // use the reduceByKey to get the sum of marks for each key and calculate the average using mapValues rdd
  val avgRddStudentGrade = nameGradeRDD.mapValues(x => (x,1)).reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2) ).mapValues(y=> 1.0 * y._1 /y._2 )
   
  //get the name as key and total marks as value using map rdd
  val nameRDD = data.map{case(name,subject,grade,marks1,marks2) =>  ((name), (marks1.toInt + marks2.toInt))}
  
  // use the reduceByKey to get the sum of marks for each key and calculate the average using mapValues rdd
  val nameRDDAVG = nameRDD.mapValues(x => (x,1)).reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2) ).mapValues(y=> 1.0 * y._1 /y._2 )

      
    // make the marks as key using keyBy rdd  
      val key_avgRddStudentGrade = avgRddStudentGrade.keyBy(ele => ele._2)
      
    // make the marks as key using keyBy rdd  
      val key_nameRDDAVG = nameRDDAVG.keyBy(ele => ele._2)
     
      /* using join rdd to get the students whose Average score per student_name across all grades is same as average score per
   			student_name per grade
      */
      val sameAvgRDD = key_avgRddStudentGrade.join(key_nameRDDAVG)
  
  }
}