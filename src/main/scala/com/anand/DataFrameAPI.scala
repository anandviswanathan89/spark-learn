import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{collect_list, udf, lit, concat_ws}
import org.apache.spark.sql.types.StringType

case class nm(name:String, time: Long, dept: Long)

object DataFrameAPIExamples extends App
{
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("DataFrameAPIExamples")
    .getOrCreate()

  val nm1 = new nm("Anand",1,2)
  val nm2 = new nm("Ranjith",145,3)
  val nm3 = new nm("Vinoth",17675,25)
  val nm4 = new nm("Adam",1456456,3)
  val nm5 = new nm("Aneesh",13453,6)
  val nm6 = new nm("Adam",13453,2)


import spark.implicits._

  //get max and avg in a single step
  val df1 = Seq(nm1,nm2,nm3,nm4,nm5,nm6).toDF()
  df1.groupBy("name").agg("time"->"max","dept"->"avg").show(truncate = false)

 /*+-------+---------+---------+
  |name   |max(time)|avg(dept)|
  +-------+---------+---------+
  |Ranjith|145      |3.0      |
  |Aneesh |13453    |6.0      |
  |Anand  |1        |2.0      |
  |Vinoth |17675    |25.0     |
  |Adam   |1456456  |2.5      |
  +-------+---------+---------+*/



  //group by and concat
  val df2 = df1.withColumn("time", $"time".cast(StringType).as("time"))
    .withColumn("dept", $"dept".cast(StringType).as("dept"))

  df2.groupBy("name")
    .agg(concat_ws(",", collect_list($"time") , collect_list($"dept")))
    .show(truncate = false)

 /* +-------+----------------------------------------------------+
  |name   |concat_ws(,, collect_list(time), collect_list(dept))|
  +-------+----------------------------------------------------+
  |Ranjith|145,3                                               |
  |Aneesh |13453,6                                             |
  |Anand  |1,2                                                 |
  |Vinoth |17675,25                                            |
  |Adam   |1456456,13453,3,2                                   |
  +-------+----------------------------------------------------+*/

}