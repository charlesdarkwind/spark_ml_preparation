import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger
import org.apache.log4j.Level

// Classe pour reprensenter les donnees du prix a une certaine heure des donnes brutes
// (prix OHLC a echantillonnage de 1 heure)
case class Kline(
    idx: Int,             // Index 0, 1, 2..., M nb examples
    date: String,         // Datetime
    open: Double,         // Prix a l'ouverture pour la periode
    high: Double,         // Prix high
    low: Double,          // Prix low
    close: Double,        // Prix a la fermeture
    volume: Double,       // Volume
    hour: Int,            // Partie heure du datetime au format 24h
    y: Double             // Le target est le prix high mais une heure dans le future
)

object ProjetObj extends App {
   Logger.getLogger("org").setLevel(Level.OFF)
   Logger.getLogger("akka").setLevel(Level.OFF)
    
  val conf = new SparkConf()
  conf.setAppName("Projet").setMaster("local")
  
  // Creation du Context Spark
  val sc = new SparkContext(conf)
   
  // SQLContext entry point pour les donnes structures
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      
  import sqlContext.implicits._

  // Creer un dataframe d'objets Kline a partir du fichier.
  val df = sc
  .textFile("/home/cloudera/ETH.csv")
  .map(_.split(","))
  .map(f => Kline(f(8).toInt, f(0), f(1).toDouble, f(2).toDouble, f(3).toDouble, f(4).toDouble, f(5).toDouble, f(6).toInt, f(7).toDouble))
  .toDF()
  
  // Afficher le schema:
  println("\nSchema du dataframe:")
  df.printSchema()
  
  // Afficher quelque lignes du dataframe:
  println("\nLes 5 premiere entrees:")
  df.head(5) map println
  
  // Describe (statistiques usuelles):
  println("\nStatistiques descriptive:")
  println(df.describe().show())
  
  /////////////////////////////////////////
  // Extraction des set de train et de test:
  
  // Index du DF pour separation test/train
  val totalLength = (df.count()).toInt
  val testIdx = (totalLength * 0.8).toInt
  
  // Extraction des predicteurs (high, low, volume, hour):
  val X_train = df.select($"high", $"low", $"volume", $"hour").filter("idx < " + testIdx)
  val X_test = df.select($"high", $"low", $"volume", $"hour").filter("idx >= " + testIdx)
  
  // Extraction des cibles (le target est le prix high, une heure dans le futur)
  val y_train = df.select($"y").filter("idx < " + testIdx)
  val y_test = df.select($"y").filter("idx >= " + testIdx)
  
  println(y_train.count())
  println(y_test.count())
  println(X_train.count())
  println(X_test.count())
}
