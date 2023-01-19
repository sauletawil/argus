// NOTE: I used the following to start the spark-shell
// spark-shell --jars "/Users/aidoc/mysql-connector-j-8.0.32/mysql-connector-j-8.0.32.jar" -c spark.ui.port=11111

import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions.{col,lit}
val spark = SparkSession.builder.appName("argus").config("spark.master", "local").getOrCreate()

val database = "lahman2016"
val user = "argus"
val password = "password"
val connString = "jdbc:mysql://localhost/" + database

def createDF(table_name: String): DataFrame = {
  val df = (spark.read.format("jdbc")
    .option("url", connString)
    .option("dbtable", table_name)
    .option("user", user)
    .option("password", password)
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .load())
  df
}

val salariesDF = createDF("Salaries")
val fieldingDF = createDF("Fielding")
val pitchingDF = createDF("Pitching")

val salaryPositionDF = (salariesDF.as("sal1")
  .join(fieldingDF, salariesDF("playerID") === fieldingDF("playerID")
    and salariesDF("yearID") === fieldingDF("yearID")
    and salariesDF("teamID") === fieldingDF("teamID")
    and salariesDF("lgID") === fieldingDF("lgID")
    , "inner")
  .select(col("sal1.yearID"), col("salary")).withColumn("position", lit("fielder"))
  .unionAll(salariesDF.as("sal2")
  .join(pitchingDF, salariesDF("playerID") === pitchingDF("playerID")
      and salariesDF("yearID") === pitchingDF("yearID")
      and salariesDF("teamID") === pitchingDF("teamID")
      and salariesDF("lgID") === pitchingDF("lgID")
      , "inner")
  .select(col("sal2.yearID"), col("salary")).withColumn("position", lit("pitcher"))))

val avgSalDF = (salaryPositionDF
  .selectExpr(
    "yearID as Year",
    "salary",
    "if(position=='fielder', salary, null) as salary_fielder",
    "if(position=='pitcher', salary, null) as salary_pitcher")
  .groupBy("Year")
  .agg(
    avg("salary_fielder"),
    avg("salary_pitcher"))
  .select(
    col("Year"),
    col("avg(salary_fielder)").as("fielding"),
    col("avg(salary_pitcher)").as("pitching"))
  .selectExpr(
    "Year",
    "format_number(round(fielding,0),0) as Fielding",
    "format_number(round(pitching,0),0) as Pitching"
  )
  .orderBy("Year"))

avgSalDF
  .write
  .mode("overwrite")
  .option("header",true)
  .csv("/tmp/spark/averageSalaries.csv")








