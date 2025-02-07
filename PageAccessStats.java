import java.io.IOException;
import java.util.HashSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageAccessStats {

    public static class AccessMapper extends Mapper<Object, Text, Text, Text> {
        private Text user = new Text();
        private Text page = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Splitting CSV line safely
            String[] tokens = value.toString().split(",");

            // Ensure we have the correct number of columns before proceeding
            if (tokens.length >= 3) {
                String userID = tokens[1].trim();   // ByWho
                String pageID = tokens[2].trim();   // WhatPage

                user.set(userID);
                page.set(pageID);
                context.write(user, page);
            }
        }
    }

    public static class AccessReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int totalAccesses = 0;
            HashSet<String> uniquePages = new HashSet<>();

            for (Text page : values) {
                totalAccesses++;
                uniquePages.add(page.toString());
            }

            String result = totalAccesses + "\t" + uniquePages.size();
            context.write(key, new Text(result));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "page access stats");
        job.setJarByClass(PageAccessStats.class);
        job.setMapperClass(AccessMapper.class);
        job.setReducerClass(AccessReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
