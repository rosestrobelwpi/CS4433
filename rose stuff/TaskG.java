import java.io.IOException;
import java.time.LocalDate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// Access access_logs.csv to look at personID and access_time
// Access pages.csv to access personID and Name
// Determine if the user hadn't logged in for 14 days
// DisconnectedCount

public class TaskG {

    public static class AccessTimeMapper extends Mapper<Object, Text, Text, Text>{
        private Text accessTime = new Text();
        private Text personID = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.startsWith("AccessID")) { // Skipping header
                return;
            }

            String[] col = value.toString().split(",");
            personID.set(col[1].trim());
            accessTime.set(col[4].trim());
            context.write(personID, accessTime);
        }
    }

    public static class PageMapper extends Mapper<Object, Text, Text, Text> {
        private Text personID = new Text();
        private Text personName = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.startsWith("PersonID")) { // Skipping header
                return;
            }
            String[] col = value.toString().split(",");
            personID.set(col[0].trim());
            personName.set(col[1].trim());
            context.write(personID, personName);
        }
    }

    public static class DisconnectedReducer extends Reducer<Text,Text,Text,Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            LocalDate today = LocalDate.now(); // Getting today's date
            LocalDate inactive = today.minusDays(14); // Subtracting 14 days from today

            boolean isActive = false;
            String name = null;
            for (Text val : values) {
                String stringVal = val.toString();
                try {
                    LocalDate accessTime = LocalDate.parse(stringVal.substring(0, 10)); // Making date of stringVal into localDate, removing the hours at the end
                    if (!accessTime.isBefore(inactive)) { // If stringVal is not before 14 days ago
                        isActive = true;
                    }
                }
                catch (Exception e) {
                    name = stringVal; // Seeing if it's a name and then setting it
                }

            }
            if (!isActive && name != null) {
                context.write(key, new Text(name));
            }

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Disconnected Count");
        job.setJarByClass(DisconnectedCount.class);
        job.setReducerClass(DisconnectedReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, AccessTimeMapper.class); // TextInputFormat reads line by line
        MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, PageMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}