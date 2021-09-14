import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Twitter {
  def main(args: Array[ String ]) {
    val conf = new SparkConf().setAppName("Tweet").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val group = sc.textFile(args(0)).map( line => { val a = line.split(",")
      (a(1).toInt,a(0).toInt) } )
    val count1 = group.groupByKey().map{case(k,v)=> var counter=0; for(i <- v){counter += 1}; (counter,1) }

    val count2 = count1.reduceByKey((x,y) => x+y )
    count2.collect().sorted.foreach(println)

    sc.stop()
  }
}
