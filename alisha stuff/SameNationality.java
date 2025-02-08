import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class SameNationality {
    public static class UserMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text userName = new Text();
        private Text userHobby = new Text();
        private String targetNationality;

        @Override
        public void setup(Context context) {
            targetNationality = context.getConfiguration().get("targetNationality", "Indian");
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            if (fields.length >= 3) {
                String name = fields[1];
                String nationality = fields[2];
                String hobby = fields[3];

                if (nationality.equalsIgnoreCase(targetNationality)) {
                    userName.set(name);
                    userHobby.set(hobby);
                    context.write(userName, userHobby);
                }
            }
    }
}

public static class UserReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder hobbies = new StringBuilder();
        for (Text val : values) {
            if (hobbies.length() > 0) {
                hobbies.append(", ");
            }
            hobbies.append(val.toString());
        }
        context.write(key, new Text(hobbies.toString()));
    }
}

public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("targetNationality", args[2]);
    Job job = Job.getInstance(conf, "nationality filter");
    job.setJarByClass(SameNationality.class);
    job.setMapperClass(UserMapper.class);
    job.setReducerClass(UserReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}