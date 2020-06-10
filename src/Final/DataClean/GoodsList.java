package Final.DataClean;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GoodsList{
	static Text v = new Text();
	static StringBuffer sb =new StringBuffer();
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
        
        if (args.length != 2) {
            System.err.println("参数错误");
            System.exit(2);
        }

        Job job = Job.getInstance();
        job.setJarByClass(GoodsList.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        Path path = new Path(args[1]);       
      	FileSystem fs = FileSystem.get(conf);        
      	if(fs.exists(path)) {            
      			fs.delete(path, true);       
      	}
        boolean y =job.waitForCompletion(true);
        
        sb.deleteCharAt(sb.length() - 1);
        FSDataOutputStream fout = fs.create(new Path("/test/test1"));
        BufferedWriter out = null;
        try {
            out = new BufferedWriter(new OutputStreamWriter(fout, "UTF-8"));
            out.write(sb.toString());
            out.newLine();
            out.flush();
        } finally {
            if (out != null) {
                out.close();
            }
        }
        //System.exit((y) ? 0 : 1);
	}

	public static class Map extends Mapper<Object, Text, Text, Text>{
		Text goods = new Text();
		Text v = new Text();
		
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] PurchaseRecord = line.split(",");
			goods.set(PurchaseRecord[1]);

            context.write(goods, v);
		}
	}
	public static class Reduce extends Reducer<Text, Text, NullWritable, Text>{

		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			 sb.append(key);
			 sb.append(",");

			 v.set(sb.toString());
			 context.write(NullWritable.get(),v);
		}
	}
}
