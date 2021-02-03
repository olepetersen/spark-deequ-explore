package explore

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SparkSession, SaveMode, SQLContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, StringType, StructField, StructType, MapType, IntegerType}
import org.apache.spark.sql.Column
import org.apache.spark.sql.types.DataType


object ConstructNestedAvro extends App {

    val spark = SparkSession.builder()
        .appName("ConstructNestedAvro")
        .config("spark.master","local")
        .getOrCreate()

    val udf_name = udf((f:String, l:String) => constructName(f,l)) 

    val basepath = "src/main/resources/data/sample-data/avro/"
    (1 to 5).foreach(n => {
        val filepath = basepath + s"userdata${n}.avro"
        println("loading "+filepath)
        val df = spark.read.format("Avro").load(filepath)

        val nameDf = df.withColumn("name", udf_name(col("first_name"),col("last_name")))

        val savefilepath = "src/main/resources/data/sample-data/avro-nested/userdata"
        nameDf.write.format("avro").mode(SaveMode.Append).save(savefilepath)     
    })

    case class name2(full: String, first: String, last: String)
    case class name(full: String, first: String, last: String, last_letters: Map[String,String], components: List[String], name: name2)

    def constructName(first: String, last: String): name = {
        var index: Int = 0
        val map: Map[String,String] = last.map(c => {
            val res = (index.toString(),c.toString())
            index = index +1
            res
        }).toMap

        val full: String = s"$first $last"
        name(full,first,last,map,List(full,first,last),name2(full,first,last))
    }

}


