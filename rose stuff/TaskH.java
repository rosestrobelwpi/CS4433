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

// Access friends.csv to look at personID and myFriend
// PopularCount

public class TaskH {

    public static enum count { // To get global count
        totalFriends,
        totalPeople
    }

    public static class FriendsMapper extends Mapper<Object, Text, Text, IntWritable>{
        private Text id = new Text();
        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.startsWith("FriendRel")) { // Skipping header
                return;
            }

            String[] col = value.toString().split(",");
            String personID = col[1];
            String myFriend = col[2];

            // Don't need to be in relation to each other
            id.set(personID);
            context.write(id, one);
            id.set(myFriend);
            context.write(id, one);
        }
    }

    public static class PopularReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result); // Output: PersonID, how many friends they have

            context.getCounter(count.totalFriends).increment(sum); // Add up to our counter to find total number of friends
            context.getCounter(count.totalPeople).increment(1); // Add up to our counter to find total number of people
        }
    }

    public static class AvgMapper extends Mapper<Object, Text, Text, IntWritable>{
        private double average;
        @Override
        protected void setup(Mapper<Object, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            average = context.getConfiguration().getDouble("avgCount", 0.0); // Getting value computed in main so that we can get the global value. avgCount is the key we retrieve the value
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tab = value.toString().split("\\t");
            String personID = tab[0];
            int myFriendCount = Integer.parseInt(tab[1]); // Back to int to compare to average

            if (myFriendCount > average) {
                context.write(new Text(personID), new IntWritable(myFriendCount));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        // Counting the amount of friends each person has
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "Counting peoples' friends");
        job1.setJarByClass(PopularCount.class);
        job1.setMapperClass(FriendsMapper.class);
        job1.setReducerClass(PopularReducer.class);
        job1.setCombinerClass(PopularReducer.class); // Since we are just summing up key value pairs, we can use combiner (associative)
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[1]));
        FileOutputFormat.setOutputPath(job1, new Path(args[2]));
//        System.exit(job1.waitForCompletion(true) ? 0 : 1);
        job1.waitForCompletion(true);

        // Need to get global variable counts after the first job is done
        long total_friends = job1.getCounters().findCounter(count.totalFriends).getValue(); // Getting count
        long total_people = job1.getCounters().findCounter(count.totalPeople).getValue(); // Getting count
        double avg = (double) total_friends / total_people; //avg

        // Output people with more friends than average
        Configuration conf2 = new Configuration();
        conf2.setDouble("avgCount", avg);
        Job job2 = Job.getInstance(conf2, "Average count");
        job2.setJarByClass(PopularCount.class);
        job2.setMapperClass(AvgMapper.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, new Path(args[2]));
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}