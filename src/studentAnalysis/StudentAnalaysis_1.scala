/*
 * 
 * Given a dataset of college students as a text file (name, subject, grade, marks) :
Dataset
Problem Statement 1:
1. Read the text file, and create a tupled rdd.
2. Find the count of total number of rows present.
3. What is the distinct number of subjects present in the entire school
4. What is the count of the number of students in the school, whose name is Mathew and
marks is 55

 */


package studentAnalysis


import org.apache.spark.SparkConf

import org.apache.spark.SparkContext

object StudentAnalaysis_1 {
   def main(args: Array[String]): Unit = {
    
  
    //creating an instance of SparkConf to provide the spark configurations.This will make spark to run in local mode
    val conf = new SparkConf().setAppName("Student Analaysis").setMaster("local")

    //Providing configuration parameter to SparkContext with an  instance of SparkConf
    val sc = new SparkContext(conf)
    
    
    
    
    /*
     * 1. Read the text file, and create a tupled rdd.
     */
    
    //split the line with delimiter ',' and create tuple 
    val fields = sc.textFile("/home/acadgild/scala_eclipse/studentDataset").map(line => line.split(",")).map(userRecord => (userRecord(0),
    userRecord(1), userRecord(2),userRecord(3).toInt,userRecord(4).toInt))
    
    /*
     * 2. Find the count of total number of rows present.
     */
    
    //fields._1 is equal to name of the student , use keyBy to make name as key and count the total number of key to get the total number of rows
    
    val countRows = fields.keyBy(fields => fields._1).keys.count()
    
    println("Total number of rows in student file  is  " + countRows)
    
    /*
     * 3. What is the distinct number of subjects present in the entire school
     */
    //field._2 is equal to subject of the student , use countByKey to  get the distinct numbers of subject 
    
    val distSubjects = fields.keyBy(field => field._2).countByKey()
    
    println("Distinct number of subjects present in the entire school are: ")
    distSubjects.foreach(println)
    
    /*
     * 4. What is the count of the number of students in the school, whose name is Mathew and
     *    marks is 55
     */

    //Use filter rdd to get the number of students whose name is Mathew and have marks 55
    val filterStudent = fields.filter(f => f._1.matches("Mathew") && (f._4 == 55 || f._5 ==55)).count()
    
    println("number of students in the school, whose name is Mathew and marks is 55 is " + filterStudent )
  
  }  
}