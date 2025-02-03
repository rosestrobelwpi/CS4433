//package org.example;

import java.io.IOException;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//Access friends.csv to look at personID and myFriend

public class PopularCount {

    public static class friendsMapper extends Mapper<Object, Text, Text, IntWritable>{
        private Text personID = new Text();
        private Text myFriend = new Text();
        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] col = value.toString().split(",");
            personID.set(col[1]);
            myFriend.set(col[2]);
            context.write(personID, one);
            context.write(myFriend, one);
        }
    }

    public static class PopularReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();
        private Map<Text, Integer> friendList = new HashMap<>();
        private int totalPeople = 0;
        private int totalFriends = 0;
        private int avgFriends = 0;

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            friendList.put(new Text(key), sum);
            totalFriends += sum;
            totalPeople++;
            avgFriends = totalFriends / totalPeople;

            if (sum > avgFriends) { //Idk what im doing
                context.write(key, new IntWritable(sum));
            }
        }
    }

//    public void debug(String[] args) throws Exception {
//        Configuration conf = new Configuration();
//        Job job = Job.getInstance(conf, "Disconnected Count");
//        job.setJarByClass(DisconnectedCount.class);
//        job.setReducerClass(DisconnectedReducer.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(Text.class);
//        FileInputFormat.addInputPath(job, new Path(args[0]));
//        FileInputFormat.addInputPath(job, new Path(args[1]));
//        FileOutputFormat.setOutputPath(job, new Path(args[2]));
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
//    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Popular Count");
        job.setJarByClass(PopularCount.class);
        job.setMapperClass(friendsMapper.class);
        job.setReducerClass(PopularReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}