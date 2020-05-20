package lab5;

import java.io.IOException;
import java.util.ArrayList;
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

public class lab5_2 {
    public lab5_2() {
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = (new GenericOptionsParser(conf, args)).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "lab5_1");
        job.setJarByClass(lab5_2.class);

        job.setMapperClass(lab5_2.TokenizerMapper.class);
//        job.setCombinerClass(lab5_2.IntSumReducer_com.class);
        job.setReducerClass(lab5_2.myReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
        job.waitForCompletion(true);
    }
    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
        private Text word = new Text();
        private Text temp_word = new Text();
        public TokenizerMapper() {
        }
        public void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
//            String fileName = ((FileSplit)context.getInputSplit()).getPath().getName();
            while(itr.hasMoreTokens()) {
                String temp = itr.nextToken();
                String temp2 = itr.nextToken();
                this.word.set(temp2);
                this.temp_word.set("child\t"+temp);
                context.write(this.word,this.temp_word);
                this.word.set(temp);
                this.temp_word.set("parent\t"+temp2);
                context.write(this.word,this.temp_word);
            }
        }
    }
    public static class myReducer extends Reducer<Text, Text, Text, Text> {
        public myReducer() {
        }
        public void reduce(Text key, Iterable<Text> values, Reducer<Text, Text,Text, Text>.Context context) throws IOException, InterruptedException {
            Text val = null;
            ArrayList<String> parent =new ArrayList();
            ArrayList<String> child =new ArrayList();
            int j=0;
            for(Iterator i$ = values.iterator(); i$.hasNext(); j++) {
                val = (Text)i$.next();
                String[] temp = val.toString().split("\t");
                int result = temp[0].compareTo("child");
                if(result==0){ child.add(temp[1]); }
                else {parent.add(temp[1]);}
            }

            // 遍历
            for(String t1:child){
                for(String t2:parent){
                    context.write(new Text(t1),new Text(t2));
                }
            }


        }
    }
}
