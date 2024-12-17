package fr.etu.polytech.movielens;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.logging.Logger;

public class HighestRatedMoviePerUserId {
    static Logger log = Logger.getLogger(
            HighestRatedMoviePerUserId.class.getName());

    public static void main(String[] args) throws Exception {
        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        System.out.println("Highest rating movie per user id");
        log.info("bonjour");
        Path input = new Path(files[0]);
        Path output = new Path(files[1]);
        System.out.println(output.toString());
        Job j = new Job(c, "movielens-highest-rated-movie-per-userId");
        j.setJarByClass(HighestRatedMoviePerUserId.class);
        j.setMapperClass(HighestRatedMoviePerUserId.MapForRating.class);
        j.setReducerClass(HighestRatedMoviePerUserId.ReduceForAverage.class);
        j.setOutputKeyClass(IntWritable.class);
        j.setOutputValueClass(MovieWritable.class);
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);
        boolean resultjob = j.waitForCompletion(true);
        System.out.println("status de fin du job" + resultjob);
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapForRating extends Mapper<LongWritable, Text, IntWritable, MovieWritable> {
        public void map(LongWritable key, Text value, Context con) throws
                IOException, InterruptedException {

            String line = value.toString();
            if ("userId,movieId,rating,timestamp".equals(line.trim())) {
                return;
            }

            String[] elements = line.split(",");
            IntWritable outputKey = new IntWritable(Integer.parseInt(elements[0]));
            MovieWritable outputValue = new MovieWritable(Integer.parseInt(elements[1]), Double.parseDouble(elements[2]));
            con.write(outputKey, outputValue);
        }
    }

    public static class ReduceForAverage extends Reducer<IntWritable, MovieWritable, IntWritable,
            IntWritable> {
        public void reduce(IntWritable userId, Iterable<MovieWritable> values, Context con)
                throws IOException, InterruptedException {
            MovieWritable movieWritable = null;
            for (MovieWritable value : values) {
                if (movieWritable == null) {
                    movieWritable = value;
                    continue;
                }
                if (value.rating > movieWritable.rating) {
                    movieWritable = value;
                }
            }
            con.write(userId, new IntWritable((movieWritable != null) ? movieWritable.movieId : -1));
        }
    }

    public static class MovieWritable implements Writable {
        private int movieId;
        private double rating;

        public MovieWritable(int movieId, double rating) {
            this.movieId = movieId;
            this.rating = rating;
        }

        public MovieWritable(){

        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeInt(movieId);
            dataOutput.writeDouble(rating);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            movieId = dataInput.readInt();
            rating = dataInput.readDouble();
        }
    }
}


