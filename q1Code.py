import sys
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: wordcount <input_folder>", file=sys.stderr)
        sys.exit(-1)

    conf = SparkConf().setAppName("python-word-count")
    sc = SparkContext(conf=conf)

    text_file = sc.textFile("hdfs://" + sys.argv[1])

    rdd1 = text_file.flatMap(lambda line: line.split(" "))
    rdd2 = rdd1.map(lambda word: (word, 1))
    rdd3 = rdd2.reduceByKey(lambda a, b: a + b)
    rdd3 = rdd3.repartition(5)
    counts = rdd3.filter(lambda x: len(x[0]) > 5)

    print("—RDD1------------------------------------")
    print(rdd1.take(5))

    print("—RDD2------------------------------------")
    print(rdd2.take(5))

    print("—RDD3------------------------------------")
    print(rdd3.take(5))

    print("—counts----------------------------------")
    print(counts.take(5))

    top_words = counts.takeOrdered(40, key=lambda x: -x[1])

    print("--------------------------------------------")
    print(*top_words, sep="\n")
    print("--------------------------------------------")
