package explore

import org.apache.spark.sql.{SparkSession,DataFrame,Column}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.SaveMode


import org.apache.hadoop.fs.LocalFileSystem
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.PathFilter

import com.amazon.deequ.analyzers.runners.AnalysisRunner
import com.amazon.deequ.analyzers.Size
import com.amazon.deequ.analyzers.Completeness
import com.amazon.deequ.metrics.{Metric,DoubleMetric}
import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.Check
import com.amazon.deequ.checks.CheckLevel
import com.amazon.deequ.checks.CheckResult

import java.util.Date
import java.sql.Time
import scala.collection.mutable.WrappedArray

object TraverseAvroDemo {

    val lfs = FileSystem.getLocal(new Configuration())

    def getSession():SparkSession = {
        SparkSession.builder.appName("TraverseDemo").config("spark.master","local").getOrCreate()
    }

    def main(args: Array[String]): Unit = {

        val currentPath = "src/main/resources/data/current_avro/checkpoints"
        val backupPath = "src/main/resources/data/backup/checkpoints"

        val spark = getSession()
        import spark.implicits._

        val dateTime = current_timestamp()
        var df:DataFrame = null
        if (!lfs.exists(new Path(currentPath))) {
            df = Seq(state("src/main/resources/data/sample-data",0,"avro")).toDF()
        } else {
            df = spark.read.format("parquet").load(currentPath)
        }

        df.show()
        
        
        val udf_mCP = udf((cp:Int) => calcCheckPoint(cp))
        val udf_mFiles = udf((location:String, old_cp:Int, new_cp:Int) => listFiles2(location, old_cp, new_cp))
        val window = Window.partitionBy("location").orderBy(desc("run_timestamp"))
        val fileDF2 = df.withColumn("rank",dense_rank().over(window))
            .where($"rank" === 1)
            .drop($"rank")
            .withColumn("run_timestamp", current_timestamp())
            .withColumn("checkpoint", $"new_checkpoint")
            .withColumn("new_checkpoint", udf_mCP($"checkpoint"))
            .withColumn("input_files", udf_mFiles($"location",$"checkpoint",$"new_checkpoint"))


        var dfList:List[DataFrame] = List()

        fileDF2.collect.foreach(r => {
            val location = r.getAs[String]("location")
            val checkpoint = r.getAs[Int]("checkpoint")
            val new_checkpoint = r.getAs[Int]("new_checkpoint")
            val datatype = r.getAs[String]("datatype")
            val run_timestamp = r.getAs[java.sql.Timestamp]("run_timestamp")
            val input_files = r.getAs[WrappedArray[String]]("input_files")
            val validation_type = r.getAs[String]("validation_type")
            val num_files = input_files.length

            val metrics = getMetrics(spark,input_files:_*)
            val validations = getValidations(spark,input_files:_*)

            dfList = Seq(state(location,checkpoint,datatype,run_timestamp,validation_type,new_checkpoint,input_files.toList,num_files,metrics,validations)).toDF() :: dfList
        })

        val finalDF = dfList.tail.foldLeft(dfList.head)((b,a) => b.union(a))

        finalDF.show(truncate=false)

        finalDF.write.format("parquet").mode(SaveMode.Append).save(currentPath)

    }


    case class state(location:String, checkpoint:Int, datatype:String, run_timestamp:java.sql.Timestamp = new java.sql.Timestamp(0), validation_type:String = "default", new_checkpoint:Int = 0, 
                 input_files:List[String] = List(), input_num:Int = 0, metrics:List[metric] = List(), validation:List[validation] = List()){}


    case class validation(check_name:String, check_level:String, check_success:Boolean,
                          constraint_name:String, constraint_success:Boolean,
                          constraint_message:String, constraint_metric:metric){}

    def checkToValidations(c:CheckResult) : List[validation] = {
        val list = List[validation]()

        val check_name = c.check.description
        val check_level = c.check.level.toString
        val check_success = c.status.toString.equals("Success")
        c.constraintResults.toList.flatMap(x => {
            val constraint_name = x.constraint.toString
            val constraint_success = x.status.toString.equals("Success")
            val constraint_message = x.message.getOrElse(null)

             if (x.metric == None) {
                List(validation(check_name, check_level, check_success,
                constraint_name, constraint_success, constraint_message,
                null))
             } else {
                val met = x.metric.get
                met.flatten.map(m => validation(
                check_name, check_level, check_success,
                constraint_name, constraint_success, constraint_message,
                createMetric(m))).toList
            }
        })
    }
    
    def getValidations(spark: SparkSession, path:String*) : List[validation] = {
        val df = spark.read.format("avro").load(path:_*)
           
        val dfColumns = df.schema.fieldNames
        val runner = VerificationSuite().onData(df)

        val sizeCheck = Check(CheckLevel.Error, "Size nonzero")
            .hasSize(_ > 0)

        val salaryCheck = Check(CheckLevel.Error, "Salary positive")
            .isNonNegative("salary")

        val completeCheck = dfColumns.foldLeft(Check(CheckLevel.Error, "Fields complete"))((b,c) => b.isComplete(c))    

        runner.addChecks(Seq(sizeCheck,salaryCheck,completeCheck))

        val analysisResult = runner.run()

        analysisResult.checkResults.values.flatMap(checkToValidations).toList
    }

    case class metric(entity:String, name:String, instance:String, success:Boolean, value:Double){}

    def createMetric(m:DoubleMetric) : metric = {
        metric(m.entity.toString,m.name,m.instance,m.value.isSuccess,m.value.getOrElse(0))
    }

    def getMetrics(spark: SparkSession, path:String*) : List[metric] = {
        println("getMetrics hmmm")
        if (spark == null || path == null){
            println("getMetrics hmmm")
            return List()
        } else {
            println(path)
        }
        val df = spark.read.format("avro").load(path:_*)
 
        println("getMetrics hmmm 2")

       val dfColumns = df.schema.fieldNames
        val runner = AnalysisRunner.onData(df)
        runner.addAnalyzer(Size())
        dfColumns.foreach((s: String) => runner.addAnalyzer(Completeness(s)))
        val analysisResult = runner.run()

        analysisResult.allMetrics
            .flatMap((m:Metric[_]) => m.flatten())
            .map(createMetric)
            .toList
    }

    case class FileStats(path:String, name:String, fileSize:Long){}

    def calcCheckPoint(checkpoint: Int) : Int = {
        checkpoint+1
    }

    def listFiles2(path: String, old_cp: Int, new_cp: Int) :  List[String] = {
        findFiles(lfs, new Path(path), new_cp)
                 .map( (s:FileStatus) => s.getPath().toString())
    }

    def isCSV(s:FileStatus): Boolean = {
        s.getPath().getName().endsWith(".avro")
    }

    def findFiles(fs: FileSystem, path: Path, checkpoint: Int): List[FileStatus] = {
        val toTrv = fs.listStatus(path,new PathFilter {
            override def accept(path: Path): Boolean = {
                val searchString = checkpoint.toString + ".avro"
                val fullPath = path.toString
                fullPath.endsWith(searchString) || fullPath.endsWith("/avro")
            }
        })

         toTrv.toList
             .flatMap( (s:FileStatus) => if (s.isDirectory()) findFiles(fs, s.getPath(),checkpoint) else List(s) )
    }

}
