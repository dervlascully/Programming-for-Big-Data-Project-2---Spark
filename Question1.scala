println("\n\nQuestion 1: Spark\n")

val githubData = spark.read.format("com.databricks.spark.csv"). option("header", "true").option("inferSchema", "true"). load("github-big-data.csv")

githubData.registerTempTable("githubTable")

val maxStars = spark.sql("select * from githubTable where stars = (select max(stars) from githubTable)")

println("Project with the most stars:")
maxStars.collect.foreach(println)

val sumStars = spark.sql("select language, sum(stars) from githubTable group by language")

println("\n\nTotal number of stars for each language:")
sumStars.collect.foreach(println)

val countContainsData = spark.sql("select count(*) from githubTable where description like '%data%'")

println("\n\nNumber of project descriptions that contain 'data':")
countContainsData.collect.foreach(println)

val countContainsDataNotNull = spark.sql("select count(*) from githubTable where description like '%data%' and language is not null")

println("\n\nNumber of project descriptions that contain 'data' and have non-null language:")
countContainsDataNotNull.collect.foreach(println)

val inputFile = sc.textFile("descriptions.txt")

val wordcount = inputFile.flatMap(line => line.split(" ")).map(word => (word,1)).reduceByKey(_ + _)

val wordcount_swap = wordcount.map(_.swap)

val highestFrequency = wordcount_swap.sortByKey(false,1).take(1)

println("\n\nWord with the highest frequency:")
print(highestFrequency.mkString(""))
println("\n")