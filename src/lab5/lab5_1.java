package lab5;

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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class lab5_1 {
    public lab5_1() {
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = (new GenericOptionsParser(conf, args)).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "lab5_1");
        job.setJarByClass(lab5_1.class);

        job.setMapperClass(lab5_1.TokenizerMapper.class);
        job.setCombinerClass(lab5_1.IntSumReducer_com.class);
        job.setReducerClass(lab5_1.myReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
        job.waitForCompletion(true);
    }
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private static final IntWritable one = new IntWritable(1);
        private Text word = new Text();
        public TokenizerMapper() {
        }
        public void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
//            String fileName = ((FileSplit)context.getInputSplit()).getPath().getName();
            while(itr.hasMoreTokens()) {
                this.word.set(itr.nextToken());
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

    public static class myReducer extends Reducer<Text, IntWritable, IntWritable, Text> {
        public myReducer() {
        }
        public int sum=1;
        public void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, IntWritable, Text>.Context context) throws IOException, InterruptedException {

            IntWritable val = null;
            int temp = sum;
            for(Iterator i$ = values.iterator(); i$.hasNext(); ) {
                val = (IntWritable)i$.next();
                context.write(new IntWritable(temp), key);
                sum+=1;
            }

        }
    }
}
