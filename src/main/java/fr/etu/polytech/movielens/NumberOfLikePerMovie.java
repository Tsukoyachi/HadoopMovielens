package fr.etu.polytech.movielens;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class NumberOfLikePerMovie {
    static Logger log = Logger.getLogger(
            NumberOfLikePerMovie.class.getName());

    public static void main(String[] args) throws Exception {
        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        System.out.println("Number of like per movie");
        log.info("bonjour");
        Path input_highestMoviePerUserId = new Path(files[0]);
        Path input_movies = new Path(files[1]);
        Path output = new Path(files[2]);
        System.out.println(output.toString());
        Job j = new Job(c, "movielens-number-of-like-per-movie");

        j.setJarByClass(NumberOfLikePerMovie.class);
        j.setReducerClass(NumberOfLikePerMovie.ReduceForLikeNumber.class);

        j.setOutputKeyClass(IntWritable.class);
        j.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(j, input_highestMoviePerUserId, TextInputFormat.class, NumberOfLikePerMovie.MapForMoviePerUser.class);
        MultipleInputs.addInputPath(j, input_movies, TextInputFormat.class, NumberOfLikePerMovie.MapForMovieName.class);

        FileOutputFormat.setOutputPath(j, output);
        boolean resultjob = j.waitForCompletion(true);
        System.out.println("status de fin du job" + resultjob);
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapForMoviePerUser extends Mapper<LongWritable, Text, IntWritable, Text> {
        public void map(LongWritable key, Text value, Context con) throws
                IOException, InterruptedException {
            String line = value.toString();
            String[] elements = line.split("\\s+");

            String userId = elements[0];
            int movieId = Integer.parseInt(elements[1]);
            con.write(new IntWritable(movieId), new Text("user:" + userId));
        }
    }

    public static class MapForMovieName extends Mapper<LongWritable, Text, IntWritable, Text> {
        public void map(LongWritable key, Text value, Context con) throws
                IOException, InterruptedException {
            String line = value.toString();
            if ("movieId,title,genres".equals(line.trim())) {
                return;
            }
            String[] elements = line.split(",");

            int movieId = Integer.parseInt(elements[0]);

            StringBuilder result = new StringBuilder();
            for (int i = 1; i <= (elements.length - 2); i++) {
                if ("".equals(result.toString())) {
                    result.append(elements[i]);
                    continue;
                }
                result.append(','+elements[i]);
            }

            con.write(new IntWritable(movieId), new Text("title:" + result));
        }
    }

    public static class ReduceForLikeNumber extends Reducer<IntWritable, Text, Text, IntWritable> {
        public void reduce(IntWritable movieId, Iterable<Text> values, Context con) throws IOException, InterruptedException {
            String title = "";
            List<Integer> userIds = new ArrayList<>();

            for (Text value : values) {
                String[] elements = value.toString().split(":");
                if ("title".equals(elements[0])) {
                    title = elements[1];
                } else {
                    userIds.add(Integer.parseInt(elements[1]));
                }
            }

            con.write(new Text(title), new IntWritable(userIds.size()));
        }
    }
}


