package explore

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SparkSession, SaveMode, SQLContext, Column}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType, MapType, IntegerType}
import org.apache.spark.sql.DataFrame

object FlattenNestedStructures extends App {

    val spark = SparkSession.builder()
        .appName("ConstructNestedAvro")
        .config("spark.master","local")
        .getOrCreate()


    val inputPath = "src/main/resources/data/sample-data/avro-nested/userdata"

    val df = spark.read.format("Avro").load(inputPath)

    println("Input schema")
    df.printSchema()

    println
    println("Flattening structs")
    val listOfCols = traverseSchema("", df.schema)
    val flattenDf = df.select(listOfCols:_*)
    flattenDf.printSchema()

    println
    println("Expanding maps to columns")

    val keyCols = mapAsColumns("name__last_letters", flattenDf)
    val flattenMapDf = flattenDf.select(col("*") :: keyCols:_*)

    flattenMapDf.printSchema()

    flattenMapDf.show()

    def mapAsColumns(column: String, df: DataFrame): List[Column] = {
        val keys = flattenDf.select(
                    explode(map_keys(col(column))).as(column + "_keys")
                ).distinct.rdd.map(r => r.getString(0)).collect.toList
        mapKeysAsColumns(column,keys)
    }

    def mapKeysAsColumns(column: String, keys: List[String]): List[Column] = {
        keys.sorted.map(k => col(column).getItem(k).as(column + "__" + k))
    }
  
    def traverseSchema(prefix: String, schema: StructType, list: List[Column] = List()): List[Column] = {
        schema match {
            case struct: StructType => struct.fields.flatMap(traverseField(prefix,_,list)).toList
            case _ => List()
        }
    }

    def traverseField(prefix: String, field: StructField, list: List[Column]): List[Column] = {
        val local_prefix = if (prefix.isEmpty) prefix else prefix + "."
        field match {
            case StructField(n, t: StructType ,_ ,_) => traverseSchema(local_prefix+n,t,list)
            case StructField(n, d: DataType ,_ ,_) => list :+ col(local_prefix + n).as((local_prefix + n).replace(".","__"))
            case _ => list
        }
    }


    // different option with List[String] of full path column names as input
    // val flattenDf = df.select("id", listOfCols:_*)
    // val flattenDf = nameDf.select(listOfCols.map(s => col(s).as(s.replace(".","__"))):_*)
    // val flattenDf = nameDf.selectExpr(listOfCols.map(s => s + " as " + s.replace(".","__")):_*)
    // def traverseSchema(prefix: String, schema: StructType, list: List[String] = List()): List[String] = {
    //     schema match {
    //         case struct: StructType => struct.fields.flatMap(traverseField(prefix,_,list)).toList
    //         case _ => List()
    //     }
    // }

    // def traverseField(prefix: String, field: StructField, list: List[String]): List[String] = {
    //     val local_prefix = if (prefix.isEmpty) prefix else prefix + "."
    //     field match {
    //         case StructField(n, t: StructType ,_ ,_) => traverseSchema(local_prefix+n,t,list)
    //         case StructField(n, d: DataType ,_ ,_) => list :+ local_prefix + n 
    //         case _ => list
    //     }
    // }
    
}


