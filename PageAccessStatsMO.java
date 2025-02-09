
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageAccessStatsMO {
    public PageAccessStatsMO() {
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "page access stats (map-only)");
        job.setJarByClass(PageAccessStatsMO.class);
        job.setMapperClass(AccessMapper.class);
        job.setNumReduceTasks(0);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class AccessMapper extends Mapper<Object, Text, Text, Text> {
        private Text output = new Text();

        public AccessMapper() {
        }

        public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            if (tokens.length >= 3) {
                String userID = tokens[1].trim();
                String pageID = tokens[2].trim();
                this.output.set(userID + "\t" + pageID);
                context.write((Object)null, this.output);
            }

        }
    }
}
