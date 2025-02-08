import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// p1: declared someone as their friend, never accessed their friend's profile
// p2: the friend
// Return ID and name of p1

// Access friends.csv to access personID and myFriend
// Access access_logs to access byWho and whatPage
// Access pages to access personID and Name

// UnviewedCount
// Only one mapper reducer

public class TaskF {

    public static class FriendsMapper extends Mapper<Object, Text, Text, Text>{
        private Text p1 = new Text(); // personID
        private Text p2 = new Text(); // myFriend

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.startsWith("FriendRel")) { // Skipping header
                return;
            }
            String[] col = value.toString().split(",");
            p1.set(col[1]);
            p2.set("Friend " + col[2]);
            context.write(p1, p2);
        }
    }

    public static class AccessMapper extends Mapper<Object, Text, Text, Text> {
        private Text byWho = new Text();
        private Text whatPage = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.startsWith("AccessID")) { // Skipping header
                return;
            }

            String[] col = value.toString().split(",");
            byWho.set(col[1]);
            whatPage.set("Accessed " + col[2]);
            context.write(byWho, whatPage);
        }
    }

    public static class PagesMapper extends Mapper<Object, Text, Text, Text> {
        private Text personID = new Text();
        private Text personName = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.startsWith("PersonID")) { // Skipping header
                return;
            }

            String[] col = value.toString().split(",");
            personID.set(col[0]);
            personName.set("Name " + col[1]);
            context.write(personID, personName);
        }
    }

    public static class UnviewedReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> friends = new ArrayList<>();
            List<String> accessedPages = new ArrayList<>();
            String name = null;
            boolean unviewed = false;

            for (Text val : values) {
                String stringVal = val.toString();
                if (stringVal.startsWith("Friend ")) {
                    friends.add(stringVal.substring(7).trim()); // Adding friendID to list
                } else if (stringVal.startsWith("Name ")) {
                    name = stringVal.substring(5).trim(); // Getting name
                } else if (stringVal.startsWith("Accessed ")) {
                    accessedPages.add(stringVal.substring(9).trim()); // Adding whatPage to list
                }
            }

            // Going through the friends list to see if they are also in the accessed list
            for (String friend : friends) {
                if (accessedPages.contains(friend)) {
                    unviewed = false;
                    break;
                } else {
                    unviewed = true;
                }
            }

            // If it is not in the list, and we have the name, then output
            if (unviewed && name != null) {
                context.write(key, new Text(name));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Unviewed Count");
        job.setJarByClass(UnviewedCount.class);
        job.setReducerClass(UnviewedReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, FriendsMapper.class); // TextInputFormat reads line by line
        MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, AccessMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[3]), TextInputFormat.class, PagesMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[4]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}