import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
// Do NOT use different Spark libraries.

object NoInOutLink {
    def main(args: Array[String]) {
        val input_dir = "sample_input"
        val links_file = input_dir + "/links-simple-sorted.txt"
        val titles_file = input_dir + "/titles-sorted.txt"
        val num_partitions = 10
       

        val conf = new SparkConf()
            .setAppName("NoInOutLink")
            .setMaster("local[*]")
            .set("spark.driver.memory", "1g")
            .set("spark.executor.memory", "2g")

        val sc = new SparkContext(conf)

        val links = sc
            .textFile(links_file, num_partitions)
           
        val outlinks = links.map(line => line.split(':')(0))
        .map(num => (num.toInt, 1))
        .reduceByKey(_+_)
       
        //outlinks.foreach(println)
               
        val inlinks = links.map(line => line.split(':')(1))
        .flatMap(link => link.split(' '))
        .filter(link => link != "")
        .map(num => (num.toInt, 1))
        .reduceByKey(_+_)
       
        //inlinks.foreach(println)

        val titles = sc
            .textFile(titles_file, num_partitions)
       
        val titles_map = titles.zipWithIndex()
        .map{ case(title, index) => ((index+1).toInt, title)}
       
        //titles_map.foreach(println)
       
        /* No Outlinks */
        println("[ NO OUTLINKS ]")
        val no_outlinks = titles_map.subtractByKey(outlinks).sortByKey()
        no_outlinks.foreach(println)

        /* No Inlinks */
        println("\n[ NO INLINKS ]")
        val no_inlinks = titles_map.subtractByKey(inlinks).sortByKey()
        no_inlinks.foreach(println)
    }
}
