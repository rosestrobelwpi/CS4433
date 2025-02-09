import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ConnectednessFactor {

    public static class FriendMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text friend = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Splitting CSV line safely
            String[] tokens = value.toString().split(",");

            // Ensure we have the correct number of columns before proceeding
            if (tokens.length >= 3) {
                String personID = tokens[1].trim();  // p1 (PersonID)
                String myFriend = tokens[2].trim();  // p2 (MyFriend)

                // Ensure PersonID and MyFriend are valid numbers and distinct
                if (!personID.equals(myFriend)) {
                    friend.set(myFriend);
                    context.write(friend, one);
                }
            }
        }
    }

    public static class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: ConnectednessFactor <input1> <output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "connectedness factor");
        job.setJarByClass(ConnectednessFactor.class);
        job.setMapperClass(FriendMapper.class);
        job.setCombinerClass(SumReducer.class);
        job.setReducerClass(SumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Add multiple input paths
        FileInputFormat.addInputPath(job, new Path(args[1]));  // friends.csv

        // Correctly set the last argument as output path
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
