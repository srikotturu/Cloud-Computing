import re
import sys
from operator import add

from pyspark.sql import SparkSession

# Function to calculate URL contributions to the rank of other URLs
def computeContribs(urls, rank):
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)

# Function to parse neighbors from URLs string
def parseNeighbors(urls):
    parts = re.split(r'\s+', urls)
    return parts[0], parts[1]

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: pagerank <file> <iterations>", file=sys.stderr)
        sys.exit(-1)

    # Create a SparkSession
    spark = SparkSession.builder.appName("PythonPageRank").getOrCreate()

    # Read input file and extract lines
    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])

    # Parse neighbors from lines, remove duplicates, and group by key
    links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache()

    # Initialize ranks for each URL to 1.0
    ranks = links.map(lambda url_neighbors: (url_neighbors[0], 1.0))

    # Perform iterations of PageRank algorithm
    for iteration in range(int(sys.argv[2])):
        # Calculate URL contributions based on neighbors and ranks
        contribs = links.join(ranks).flatMap(
            lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))

        # Update ranks based on contributions
        ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)

    # Print the final ranks for each URL
    for (link, rank) in ranks.collect():
        print("%s has rank: %s." % (link, rank))

    # Stop the SparkSession
    spark.stop()
