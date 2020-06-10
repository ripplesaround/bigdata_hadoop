package DataClean;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DataClean{
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
        
        if (args.length != 2) {
            System.err.println("参数错误");
            System.exit(2);
        }

        Job job = Job.getInstance();
        job.setJarByClass(DataClean.class);
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
      	//System.exit((y) ? 0 : 1);
	}

	public static class Map extends Mapper<Object, Text, Text, Text>{
		Text user = new Text();
		Text v = new Text();
		
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] PurchaseRecord = line.split(",");
			user.set(PurchaseRecord[0]);

            v.set(PurchaseRecord[1]+","+PurchaseRecord[2]+","+PurchaseRecord[3]);
            context.write(user, v);
		}
	}
	public static class Reduce extends Reducer<Text, Text, NullWritable, Text>{
		Text v = new Text();
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			StringBuffer sb =new StringBuffer();
			String gender = new String();
			String age = new String();
			sb.append(key);
			 sb.append(" ");
			 for (Text value : values) {
				 	String line = value.toString();
					String[] temp = line.split(",");		
					age=temp[1];	
					gender=temp[2];
					sb.append(temp[0]);
					sb.append(",");
	          }
			 sb.deleteCharAt(sb.length() - 1);
			 sb.append(" ");
			 sb.append(age);
			 sb.append(" ");
			 sb.append(gender);
			 v.set(sb.toString());
			 context.write(NullWritable.get(),v);
		}
	}
	 
}
