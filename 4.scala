// Databricks notebook source
// DBTITLE 1,Practical 4 (Using DataFrames and DataSets in Scala
/*Login databricks
create ->cluster ->cluster name = prc4 ->create cluster
create ->notebook ->Name=scala ->default language = scala ->create*/

// COMMAND ----------

/*google ->search ->docs.databricks.com ->Data Science & Engineering  ->DataFrames and Datasets ->Introduction to Datasets */

// COMMAND ----------

/*Create a Dataset

To convert a sequence to a Dataset, call .toDS() on the sequence.*/

val dataset = Seq(1, 2, 3).toDS()
dataset.show()

// COMMAND ----------

/*If you have a sequence of case classes, calling .toDS() provides a Dataset with all the necessary fields.*/

case class Person(name: String, age: Int)

val personDS = Seq(Person("Max", 33), Person("Adam", 32), Person("Muller", 62)).toDS()
personDS.show()

// COMMAND ----------

/*Without instances*/

val personDS = Seq(("Max", 33), ("Adam", 32), ("Muller", 62)).toDS()
personDS.show()

// COMMAND ----------

/*Create a Dataset from an RDD

To convert an RDD into a Dataset, call rdd.toDS().*/

val rdd = sc.parallelize(Seq((1, "Spark"), (2, "Databricks")))
val integerDS = rdd.toDS()
integerDS.show()

// COMMAND ----------

/*Create a Dataset from a DataFrame

You can call df.as[SomeCaseClass] to convert the DataFrame to a Dataset.*/

case class Company(name: String, foundingYear: Int, numEmployees: Int)
val inputSeq = Seq(Company("ABC", 1998, 310), Company("XYZ", 1983, 904), Company("NOP", 2005, 83))
val df = sc.parallelize(inputSeq).toDF()

val companyDS = df.as[Company]
companyDS.show()

// COMMAND ----------

/*You can also deal with tuples while converting a DataFrame to Dataset without using a case class.*/

val rdd = sc.parallelize(Seq((1, "Spark"), (2, "Databricks"), (3, "Notebook")))
val df = rdd.toDF("Id", "Name")

val dataset = df.as[(Int, String)]
dataset.show()

// COMMAND ----------

// DBTITLE 1,Work with Datasets
/*Word Count Example */

val wordsDataset = sc.parallelize(Seq("Spark I am your father", "May the spark be with you", "Spark I am your father")).toDS()
val groupedDataset = wordsDataset.flatMap(_.toLowerCase.split(" "))
                                 .filter(_ != "")
                                 .groupBy("value")
val countsDataset = groupedDataset.count()
countsDataset.show()


// COMMAND ----------

// DBTITLE 1,read text files from a directory into RDD
/*google ->rdd textfile ->1st site*/

/*create a textfile on desktop ->*/

val rddFromFile =spark.sparkContext.textFile("/FileStore/tables/zain.txt")
rddFromFile.collect().foreach(f=>{
println(f)
})

// COMMAND ----------

// DBTITLE 1,count
rddFromFile.count()

// COMMAND ----------

rddFromFile.first()

// COMMAND ----------

val lineswithHDFS= rddFromFile.filter(line => line.contains("HDFS"))

// COMMAND ----------

lineswithHDFS.collect().take(1).foreach(println)

// COMMAND ----------

// DBTITLE 1,Convert RDD into DataSet
val dataset1=rddFromFile.toDS()

// COMMAND ----------

// DBTITLE 1,Convert a Dataset to a DataFrame 
import org.apache.spark.sql.functions._

val wordsDataset = sc.parallelize(Seq("Spark I am your father", "May the spark be with you", "Spark I am your father")).toDS()
val result = wordsDataset
              .flatMap(_.split(" "))               // Split on whitespace
              .filter(_ != "")                     // Filter empty words
              .map(_.toLowerCase())
              .toDF()                              // Convert to DataFrame to perform aggregation / sorting
              .groupBy($"value")                   // Count number of occurrences of each word
              .agg(count("*") as "numOccurances")
              .orderBy($"numOccurances" desc)      // Show most common words first
result.show()

// COMMAND ----------

val Dataset2 = wordsDataset.flatMap(_.toLowerCase.split(" "))
                                 .filter(_ != "")
                                 .groupBy("value")
val counts = Dataset2.count()
counts.show()

// COMMAND ----------


