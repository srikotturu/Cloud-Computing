val lines = sc.textFile("hdfs:///mydata/input.txt") // Read input text file from HDFS

val links = lines.map{ s =>
    val parts = s.split("\\s+")
    (parts(0), parts(1))
}.distinct().groupByKey().cache() // Split lines, create key-value pairs, remove duplicates, group by key, and cache the result

var ranks = links.mapValues(v => 1.0) // Initialize ranks with a value of 1.0 for each key

for (i <- 1 to 2) {
    val contribs = links.join(ranks).values.flatMap{ case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
    } // Join links and ranks, calculate contributions, and flatten the result

    ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _) // Reduce contributions by key, apply rank formula

}

val output = ranks.collect() // Collect the final ranks

output.foreach(tup => println(tup._1 + " has rank: " + tup._2 + ".")) // Print each key and its rank

sc.stop() // Stop the SparkContext
