package explore

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SparkSession, SaveMode, SQLContext}
import org.apache.spark.sql.functions._

import com.amazon.deequ.analyzers.runners.{AnalysisRunner, AnalyzerContext}
import com.amazon.deequ.analyzers.runners.AnalyzerContext.successMetricsAsDataFrame
import com.amazon.deequ.analyzers.{Compliance, Correlation, Size, Completeness, Mean, ApproxCountDistinct}

import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import com.amazon.deequ.{VerificationSuite, VerificationResult}
import com.amazon.deequ.VerificationResult.successMetricsAsDataFrame
import com.amazon.deequ.checks.Check
import com.amazon.deequ.checks.CheckLevel
import com.amazon.deequ.metrics.DoubleMetric
import com.amazon.deequ.metrics.Metric
import com.amazon.deequ.checks.CheckResult


object DeequDemo {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder().appName("DeequDemo").config("spark.master","local").getOrCreate()


        // val fs: LocalFileSystem = FileSystem.getLocal(new Configuration())
       
        // while (files.hasNext()) {
        //     val lfs = files.next()
        //     println(lfs)
        // }

        val moviesDF = spark.read
            .format("json")
            .options(Map(
                "inferSchema" -> "true",
                "path" -> "src/main/resources/data/movies.json"
            ))
            .load()  

        moviesDF.cache()

        moviesDF.select(countDistinct(col("Major_Genre"))).show()

        val selectColumns2 = moviesDF.schema.fieldNames
        println("second")
        selectColumns2.foreach(println)

        moviesDF.printSchema()
            // root
            // |-- Creative_Type: string (nullable = true)
            // |-- Director: string (nullable = true)
            // |-- Distributor: string (nullable = true)
            // |-- IMDB_Rating: double (nullable = true)
            // |-- IMDB_Votes: long (nullable = true)
            // |-- MPAA_Rating: string (nullable = true)
            // |-- Major_Genre: string (nullable = true)
            // |-- Production_Budget: long (nullable = true)
            // |-- Release_Date: string (nullable = true)
            // |-- Rotten_Tomatoes_Rating: long (nullable = true)
            // |-- Running_Time_min: long (nullable = true)
            // |-- Source: string (nullable = true)
            // |-- Title: string (nullable = true)
            // |-- US_DVD_Sales: long (nullable = true)
            // |-- US_Gross: long (nullable = true)
            // |-- Worldwide_Gross: long (nullable = true)

        val runner = AnalysisRunner.onData(moviesDF)

        runner.addAnalyzer(Size())

        selectColumns2.foreach((s: String) => runner.addAnalyzer(Completeness(s)))

        val analysisResult = runner.run()

        // val analysisResult: AnalyzerContext = { AnalysisRunner
        //     // data to run the analysis on
        //     .onData(moviesDF)
        //     // define analyzers that compute metrics
        //     .addAnalyzer(Size())
        //     .addAnalyzer(Completeness("review_id")
        //     .addAnalyzer(ApproxCountDistinct("review_id"))
        //     .addAnalyzer(Mean("star_rating"))
        //     .addAnalyzer(Compliance("top star_rating", "star_rating >= 4.0"))
        //     .addAnalyzer(Correlation("total_votes", "star_rating"))
        //     .addAnalyzer(Correlation("total_votes", "helpful_votes"))
        //     // compute metrics
        //     .run()
        // }

        analysisResult.metricMap.map(_._2).foreach((x:Metric[_]) => println(x.entity, x.instance, x.name, x.value.toString))

        // // retrieve successfully computed metrics as a Spark data frame
        // val metrics = AnalyzerContext.successMetricsAsDataFrame(spark, analysisResult)

        // metrics.show(100)

        // // metrics.write.format("json").mode(SaveMode.Append).save("src/main/resources/data/metrics01")


        val cSanity = Check(CheckLevel.Error, "Sanity")
            .hasSize(_ > 0)
            .isNonNegative("US_DVD_Sales")
        val cUnique = Check(CheckLevel.Error, "Uniqueness")
            .isUnique("Title")
            .isUnique("US_DVD_Sales")
        val vResult = VerificationSuite().onData(moviesDF).addChecks(Seq(cSanity,cUnique)).run()

        vResult.checkResults.foreach(x => {println(x._1); println(x._2)})
        println

        vResult.checkResults.values.foreach(println)
            println
        // CheckResult(Check(Error,Sanity,List(SizeConstraint(Size(None)), ComplianceConstraint(Compliance(US_DVD_Sales is non-negative,COALESCE(US_DVD_Sales, 0.0) >= 0,None)))),Success,List(ConstraintResult(SizeConstraint(Size(None)),Success,None,Some(DoubleMetric(Dataset,Size,*,Success(3201.0)))), ConstraintResult(ComplianceConstraint(Compliance(US_DVD_Sales is non-negative,COALESCE(US_DVD_Sales, 0.0) >= 0,None)),Success,None,Some(DoubleMetric(Column,Compliance,US_DVD_Sales is non-negative,Success(1.0))))))
        // CheckResult(Check(Error,Uniqueness,List(UniquenessConstraint(Uniqueness(List(Title),None)), UniquenessConstraint(Uniqueness(List(US_DVD_Sales),None)))),Error,List(ConstraintResult(UniquenessConstraint(Uniqueness(List(Title),None)),Failure,Some(Value: 0.985 does not meet the constraint requirement!),Some(DoubleMetric(Column,Uniqueness,Title,Success(0.985)))), ConstraintResult(UniquenessConstraint(Uniqueness(List(US_DVD_Sales),None)),Success,None,Some(DoubleMetric(Column,Uniqueness,US_DVD_Sales,Success(1.0))))))


        vResult.checkResults.values.flatMap(c => c.constraintResults).foreach(c => println(c.status, c.message, c))
        println

        vResult.checkResults.values.flatMap(c => c.constraintResults)
            .foreach(c => println(c.status, c.message, c.constraint.toString, c.metric) )

        // val verifications = VerificationResult.successMetricsAsDataFrame(spark, vResult)

        // val cResults = VerificationResult.checkResultsAsDataFrame(spark,vResult)

        // // println("VerificationStatus value: " + vResult.status)

        // // verifications.write.format("json").mode(SaveMode.Append).save("src/main/resources/data/verifications01")

        // cResults.write.format("json").mode(SaveMode.Append).save("src/main/resources/data/checkresults01")

    }
  
}
