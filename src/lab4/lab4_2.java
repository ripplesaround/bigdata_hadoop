package lab4;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
public class lab4_2 {
    public lab4_2() {
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = (new GenericOptionsParser(conf, args)).getRemainingArgs();
        if(otherArgs.length < 2) {
            System.err.println("Usage: lab4.lab4_2 <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "lab4_2_1");
        job.setJarByClass(lab4_2.class);
        job.setMapperClass(lab4_2.TokenizerMapper.class);
        job.setCombinerClass(lab4_2.IntSumReducer_com.class);
        job.setReducerClass(lab4_2.IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        for(int i = 0; i < otherArgs.length - 2; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 2]));
        job.waitForCompletion(true);

        Job job2 = Job.getInstance(conf,"lab4_2_2");
        //job2.setJarByClass(inverseTest.class);
        job2.setMapperClass(job2Mapper.class);
        job2.setReducerClass(job2reducer.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job2, new Path(otherArgs[otherArgs.length - 2]));
        FileOutputFormat.setOutputPath(job2, new Path(otherArgs[otherArgs.length - 1]));

        System.exit(job2.waitForCompletion(true)?0:1);

    }
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private static final IntWritable one = new IntWritable(1);
        private Text word = new Text();
        public TokenizerMapper() {
        }
        public void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            String fileName = ((FileSplit)context.getInputSplit()).getPath().getName();
            while(itr.hasMoreTokens()) {
                this.word.set(itr.nextToken()+"\t"+fileName);
                context.write(this.word, one);
            }
        }
    }

    public static class IntSumReducer_com extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        public IntSumReducer_com() {
        }
        public void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            int sum = 0;
            IntWritable val;
            for(Iterator i$ = values.iterator(); i$.hasNext(); sum += val.get()) {
                val = (IntWritable)i$.next();
            }
            this.result.set(sum);
            context.write(key, this.result);
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        public IntSumReducer() {
        }
        public void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            int sum = 0;
            IntWritable val;
            for(Iterator i$ = values.iterator(); i$.hasNext(); sum += val.get()) {
                val = (IntWritable)i$.next();
                context.write(key, val);
            }
        }
    }

    public static class job2Mapper extends Mapper<Object, Text, Text, Text> {

        public job2Mapper() {
        }
        public void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            String[] args = value.toString().split("\t");
            if (args != null && args.length == 3) {
                context.write(new Text(args[0]), new Text(args[1] + " - " + args[2]));
            }
        }
    }
    public static class job2reducer extends Reducer<Text, Text, Text, Text> {
        private IntWritable result = new IntWritable();
        public job2reducer() {
        }
        public void reduce(Text key, Iterable<Text> values, Reducer<Text, Text,Text,Text>.Context context) throws IOException, InterruptedException {
            String temp = "";
            for(Text value:values) {
                if (temp.length()>0) {
                    temp += " , ";
                }
                temp += value;
            }
            context.write(key, new Text(temp));
        }
    }
}