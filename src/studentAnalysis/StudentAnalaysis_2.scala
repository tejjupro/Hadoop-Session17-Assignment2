                                                                                                                                                                                                                                                                                                                                                                                                                                                  

/*
 * 
 * Given a dataset of college students as a text file (name, subject, grade, marks) :
Dataset
Problem Statement 2:
1. What is the count of students per grade in the school?
2. Find the average of each student (Note - Mathew is grade-1, is different from Mathew in
some other grade!)
3. What is the average score of students in each subject across all grades?
4. What is the average score of students in each subject per grade?
5. For all students in grade-2, how many have average score greater than 50?

 */

package studentAnalysis

import org.apache.spark.SparkConf

import org.apache.spark.SparkContext

object StudentAnalaysis_2 {
  def main(args: Array[String]): Unit = {
    
  
      //creating an instance of SparkConf to provide the spark configurations.This will make spark to run in local mode
    val conf = new SparkConf().setAppName("Student Analaysis").setMaster("local")

    //Providing configuration parameter to SparkContext with an  instance of SparkConf
    val sc = new SparkContext(conf)
    

    
    //split the line with delimiter ',' and create tuple RDD
    
    val fields = sc.textFile("/home/acadgild/scala_eclipse/studentDataset").map(line => line.split(",")).map(userRecord => (userRecord(0),
    userRecord(1), userRecord(2),userRecord(3).toInt,userRecord(4).toInt))
  

    /*
    * 1. What is the count of students per grade in the school?
    */ 

    //use the reduceByKey rdd to calculate the number of students per grade
    
    val studentsPerGrade = fields.map{case(name,subject,grade,marks1,marks2) =>  (grade, 1)}.reduceByKey(_ + _ )
    
    studentsPerGrade.collect.foreach(println)
    
    println("count of students per grade in the school :"  )
    
    
    
    /*
     * 2. Find the average of each student (Note - Mathew in grade-1, is different from Mathew in
 		 *   	some other grade!)
     * 
     */
   
     //get the name and grade as key and total marks as value using map rdd
    val nameGradeRDD = fields.map{case(name,subject,grade,marks1,marks2) =>  ((name,grade), (marks1.toInt + marks2.toInt))}//.reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2) )
     
    // use the reduceByKey to get the sum of marks for each key and calculate the average using mapValues rdd
    val avgRddStudentGrade = nameGradeRDD.mapValues(x => (x,1)).reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2) ).mapValues(y=> 1.0 * y._1 /y._2 )
      
    avgRddStudentGrade.collect.foreach(println)
    
    println("Average of each student per grade " )
    
    
    /*
     * 
     * 3. What is the average score of students in each subject across all grades?
     * 
     * 
     */
    
    //get the name and subject as key and total marks as value using map rdd
    
    val studentSubjectRdd = fields.map{case(name,subject,grade,marks1,marks2) =>  ((name,subject), (marks1.toInt + marks2.toInt))}//.reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2) )

    // use the reduceByKey to get the sum of marks for each key and calculate the average using mapValues rdd
     val studentSubjectRddAVG = studentSubjectRdd.mapValues(x => (x,1)).reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2) ).mapValues(y=> 1.0 * y._1 /y._2 )

    
     studentSubjectRddAVG.collect.foreach(println)
     
    println("Average score of students in each subject across all grades " )
    
    
    
    
    /*
     * 4. What is the average score of students in each subject per grade?
     */
     
    //get the name , subject and grade as key and total marks as value using map rdd
    
     val studentSubjectGradeRdd = fields.map{case(name,subject,grade,marks1,marks2) =>  ((name,subject,grade), (marks1.toInt + marks2.toInt))}//.reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2) )
     
     // use the reduceByKey to get the sum of marks for each key and calculate the average using mapValues rdd
     val studentSubjectGradeRddAVG = studentSubjectGradeRdd.mapValues(x => (x,1)).reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2) ).mapValues(y=> 1.0 * y._1 /y._2 )
    
     studentSubjectGradeRddAVG.collect.foreach(println)
     
     println("Average score of students in each subject per grade  " )
     
     
     /*
      * 5. For all students in grade-2, how many have average score greater than 50?
      * 
      * 
      */
     //Filter the students whose grade is 'grade-2' and the average score greater than 50 using filter rdd
      
     val filterStudents =avgRddStudentGrade.filter(f => f._1._2.contains("grade-2") && f._2 >50)     
    
     filterStudents.collect.foreach(println)
      
     println("For all students in grade-2, students having  average score greater than 50 : ")
     
  }

}