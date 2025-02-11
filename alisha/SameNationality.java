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

//PersonID,Name,Nationality,Country Code,Hobby
//        0,Amy Garcia,Vanuatu,93,Storm Chasing
//        1,Andrew Owens,Tuvalu,221,Sporting dog field trials
//        2,Donna Miller,Ecuador,685,Modeling Ships


public class SameNationality {
    public static class UserMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text userName = new Text();
        private Text userHobby = new Text();
        private String targetNationality;

        @Override
        public void setup(Context context) {
            targetNationality = context.getConfiguration().get("targetNationality", "India");
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length >= 4) {
                String name = fields[1];
                String nationality = fields[2];
                String hobby = fields[4];

                System.out.println("Processing: " + name + ", " + nationality + ", " + hobby);

                if (nationality.equalsIgnoreCase(targetNationality)) {
                    userName.set(name);
                    userHobby.set(hobby);
                    context.write(userName, userHobby);
                    System.out.println("Emitting: " + name + " -> " + hobby);
                }
            }
        }
    }


    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: SameNationality <input path> <output path> <target nationality>");
            System.exit(-1);
        }
        Configuration conf = new Configuration();
        conf.set("targetNationality", args[2]);
        Job job = Job.getInstance(conf, "nationality filter");
        job.setJarByClass(SameNationality.class);
        job.setMapperClass(UserMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}