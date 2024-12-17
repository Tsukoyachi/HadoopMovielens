package fr.etu.polytech.movielens;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class GroupByNumberOfLikePerMovie {
    static Logger log = Logger.getLogger(
            GroupByNumberOfLikePerMovie.class.getName());

    public static void main(String[] args) throws Exception {
        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        System.out.println("Group By number of like per movie");
        log.info("bonjour");
        Path input = new Path(files[0]);
        Path output = new Path(files[1]);
        System.out.println(output.toString());
        Job j = new Job(c, "movielens-group-bynumber-of-like-per-movie");

        j.setJarByClass(GroupByNumberOfLikePerMovie.class);

        j.setMapperClass(GroupByNumberOfLikePerMovie.MapForLikeNumber.class);
        j.setReducerClass(GroupByNumberOfLikePerMovie.ReduceForGroupBy.class);

        j.setOutputKeyClass(IntWritable.class);
        j.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(j, input);

        FileOutputFormat.setOutputPath(j, output);
        boolean resultjob = j.waitForCompletion(true);
        System.out.println("status de fin du job" + resultjob);
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapForLikeNumber extends Mapper<LongWritable, Text, IntWritable, Text> {
        public void map(LongWritable key, Text value, Context con) throws
                IOException, InterruptedException {
            String line = value.toString();
            String[] elements = line.split("\\s+");

            int likeNumber = Integer.parseInt(elements[elements.length-1]);
            StringBuilder title = new StringBuilder();
            for (int i = 0; i < elements.length - 1; i++) {
                title.append(elements[i]).append(" ");
            }

            con.write(new IntWritable(likeNumber), new Text(title.toString().trim()));
        }
    }

    public static class ReduceForGroupBy extends Reducer<IntWritable, Text, IntWritable, Text> {
        public void reduce(IntWritable likeNumber, Iterable<Text> values, Context con) throws IOException, InterruptedException {
            List<String> titles = new ArrayList<>();

            for (Text value : values) {
                titles.add(value.toString());
            }

            String title = titles.isEmpty() ? "" : String.join(" ", titles).trim();

            con.write(likeNumber, new Text(title));
        }
    }
}


