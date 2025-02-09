//find the top 10 popular Facebook pages, namely, those that were the most often accessed based on your AccessLog dataset compared to all other pages. return their id, name and nationality


import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Comparator;
import java.util.Collections;
import java.util.List;

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

public class PopularPages {
    public static class PageMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text pageId = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\\s+");
            if (fields.length > 1) {
                pageId.set(fields[1]);
                context.write(pageId, one);
            }
        }
    }

    public static class PageReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            pageCounts.put(key.toString(), sum);
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            PriorityQueue<Map.Entry<String, Integer>> pq = new PriorityQueue<>(Comparator.comparingInt(Map.Entry::getValue));
                for (Map.Entry<String, Integer> entry : pageCounts.entrySet()) {
                    pq.add(entry);
                    if (pq.size() > 10) {
                        pq.poll();
                    }
                }
                Map<String, String[]> lookup = loadLookupFile();
                while (!pq.isEmpty()) {
                    Map.Entry<String, Integer> entry = pq.poll();
                    String pageId = entry.getKey();
                    String pageName = lookup.containsKey(pageId) ? lookup.get(pageId)[0] : "Unknown";
                    String nationality = lookup.containsKey(pageId) ? lookup.get(pageId)[1] : "Unknown";
                    context.write(new Text(pageId + "\t" + pageName + "\t" + nationality), new IntWritable(entry.getValue()));
                }
            }

            private Map<String, String[]> loadLookupFile () throws IOException {
                Map<String, String[]> lookup = new HashMap<>();
                try (BufferedReader reader = new BufferedReader(new FileReader("lookup.txt"))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        String[] fields = line.split("\t");
                        if (fields.length == 3) {
                            lookup.put(fields[0], new String[]{fields[1], fields[2]});
                        }
                    }
                }
                return lookup;
            }
        }

        public static void main(String[] args) throws Exception {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "popular pages");
            job.setJarByClass(PopularPages.class);
            job.setMapperClass(PageMapper.class);
            job.setReducerClass(PageReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
    }


