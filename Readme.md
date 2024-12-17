# Lab 2 - Hadoop

Author : **Axel Delille**
Project from **Polytech Nice Sophia** given by **Fabrice Huet**.

In this readme, I won't cover the content of the hadoop configuration as it's not mine, if you want to use the docker compose and launch hadoop/hdfs, just modify the hostname and up the docker compose, it should work, if it don't you're on your own :/

The goal of the project is from the inputs in hadoop/movielens/movies.csv and hadoop/movielens/ratings.csv to sort the movies by ascending number of like using hadoop Map Reduce.
For that this project contains 3 java files :
- HighestRatedMoviePerUserId : It take in parameter the ratings.csv file path on hdfs and an output path on hdfs and create a file where we have each userId with their favorite movieId (the first one that come in the reduce).
- NumberOfLikePerMovie : It take in parameter the output path of the first program on hdfs, the path of the movies.csv on hdfs and the output path on hdfs too and create a file where we have the title of each movie with the number of people who put it as their favorite movie (number of likes).
- GroupByNumberOfLikePerMovie : It take in parameter the output path of the previous program in hdfs and an output path on hdfs and the create a file where we have the equivalent of a groupBy SQL query across the previous file to group each movie who possess the same number of likes.

Here is an example of commands to run these 3 code, must be done in order without any folder named input, output or movielens in your hdfs to ensure that they're is no error at runtime :

1. You first need the files movies.csv and ratings.csv in the file downloadable [here](https://files.grouplens.org/datasets/movielens/ml-32m.zip), it is not included due to the size of the ratings.csv that is superior to 100mb and require git-lfs to be included in the repository. Put these files in a new folder at ./hadoop/movielens, it'll be accessible from the namenode docker container instanciated throug hthe docker compose.

2. If you want to recompile the jar in the resourcemanager directory you need a java version 8 or more installation and a maven installation, then you can do :

```java
maven clean package
```
You can then replace the jar in ./resourcemanager with the generated ./target/Lab-2-Hadoop-1.0-SNAPSHOT.jar to execute it.

3. On the namenode :
```bash
hdfs dfs -put /hadoop/labs/movielens/ /movielens
```

4. On the resourcemanager :
```bash
hadoop jar /hadoop/labs/HadoopMovieLens-1.0.jar \
    fr.etu.polytech.movielens.HighestRatedMoviePerUserId \
    /input/ratings.csv \
    /output/favoriteMoviePerUser
hadoop jar /hadoop/labs/HadoopMovieLens-1.0.jar \
    fr.etu.polytech.movielens.NumberOfLikePerMovie \
    /output/favoriteMoviePerUser \
    /input/movies.csv \
    /output/numberOfLikePerMovie
hadoop jar /hadoop/labs/HadoopMovieLens-1.0.jar \
    fr.etu.polytech.movielens.GroupByNumberOfLikePerMovie \
    /output/numberOfLikePerMovie \
    /output/groupByNumberOfLikePerMovie
```

5. Then the result is in "/output/groupByNumberOfLikePerMovie" directory on the hdfs in the file part-r-00000. You can download it from the UI or do this command :

```bash
hdfs dfs -cat /output/groupByNumberOfLikePerMovie/part-r-00000
```