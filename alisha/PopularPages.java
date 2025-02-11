import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PopularPages {

    public static class PageAccessMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text pageID = new Text();
        private final static IntWritable one = new IntWritable(1);

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Processing AccessLog entries
            String[] fields = value.toString().split(",");
            if (fields.length > 0) {
                pageID.set(fields[0]);  // PageID is the first field
                context.write(pageID, one);
            }
        }
    }

    public static class PageDetailsMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text pageID = new Text();
        private Text pageDetails = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Processing Pages dataset
            String[] fields = value.toString().split(",");
            if (fields.length == 3) {
                pageID.set(fields[0]);  // PageID is the first field
                pageDetails.set(fields[1] + "," + fields[2]);  // PageName and Nationality
                context.write(pageID, pageDetails);
            }
        }
    }

    public static class PopularPagesReducer extends Reducer<Text, Text, Text, IntWritable> {
        private final static IntWritable totalCount = new IntWritable();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int totalAccessCount = 0;
            String pageName = "";
            String nationality = "";

            for (Text value : values) {
                String[] parts = value.toString().split(",");
                if (parts.length == 1) {  // This is the count from the AccessLog
                    totalAccessCount++;
                } else if (parts.length == 2) {  // This is PageDetails from Pages dataset
                    pageName = parts[0];
                    nationality = parts[1];
                }
            }

            context.write(new Text(pageName + "," + nationality), new IntWritable(totalAccessCount));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Popular Facebook Pages");
        job.setJarByClass(PopularPages.class);

        // Multiple inputs (AccessLog and Pages)
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, PageAccessMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PageDetailsMapper.class);

        job.setReducerClass(PopularPagesReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

