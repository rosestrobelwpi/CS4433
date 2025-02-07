import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageAccessStatsMO {

    public static class AccessMapper extends Mapper<Object, Text, Text, Text> {
        private Text output = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Splitting CSV line safely
            String[] tokens = value.toString().split(",");

            // Ensure we have the correct number of columns
            if (tokens.length >= 3) {
                String userID = tokens[1].trim();  // ByWho
                String pageID = tokens[2].trim();  // WhatPage

                // Emit a combined output string with user stats (total accesses, unique pages)
                output.set(userID + "\t" + pageID);
                context.write(null, output);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "page access stats (map-only)");
        job.setJarByClass(PageAccessStatsMO.class);
        job.setMapperClass(AccessMapper.class);
        job.setNumReduceTasks(0);  // No reducer - map-only job
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
