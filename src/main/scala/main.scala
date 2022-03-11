import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

case class SongPlayers(song: String, firsts: Seq[String], seconds: Seq[String], thirds: Seq[String])
case class PersonCount(person: String, count: Int)
object main{
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Aggregator")
      .config("spark.master", "local")
      .getOrCreate()
    import spark.implicits._
    val filename = "Gwarek2022.csv"
    val playsDF = spark.read.option("header", "true").csv(filename)
    val piecesDF = playsDF.groupBy("Song")
      .agg(
        collect_list("First").as("firsts"),
        collect_list("Second").as("seconds"),
        collect_list("Third").as("thirds"))
      .as[SongPlayers]
      .map(row => {
        row.firsts.map(a => PersonCount(a,1)).groupBy(_.person)
          .mapValues(l => l.reduce((p1, p2) => PersonCount(p1.person, p1.count + p2.count)))


      })
      .show(truncate = false)
    playsDF.show()
  }
}
