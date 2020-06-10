package Final.DataClean;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UserGoodsMatrix {
	static String v = new String();
	static String[] goodslist;
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
        
        if (args.length != 3) {
            System.err.println("参数错误");
            System.exit(2);
        }

        Job job = Job.getInstance();
        job.setJarByClass(UserGoodsMatrix.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        
        Path path = new Path(args[2]);       
  		FileSystem fs = FileSystem.get(conf);        
  		if(fs.exists(path)) {            
  			fs.delete(path, true);       
  		}
      	path = new Path(args[1]);
      	FSDataInputStream fin = fs.open(path);
      	BufferedReader  in = new BufferedReader(new InputStreamReader(fin,"UTF-8"));
      	v = in.readLine();
      	goodslist=v.split(",");
      	
      	path = new Path(args[2]);       
      	if(fs.exists(path)) {            
      			fs.delete(path, true);       
      	}
      	boolean y=job.waitForCompletion(true);
        //System.exit((y) ? 0 : 1);
	}
	
	public static class Map extends Mapper<Object, Text, Text, Text>{
		Text user=new Text();
		Text age=new Text();
		Text gender=new Text();
		Text v =new Text();
		
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String line = value.toString();
			String[] Mesg = line.split(" ");
			
			user.set(Mesg[0]);
			String[] goods =Mesg[1].split(",");
			age.set(Mesg[2]);
			gender.set(Mesg[3]);
			
			StringBuffer sb =new StringBuffer();
			sb.append(user);
			sb.append(" ");
			for (String x:goodslist){
				for (String y:goods){
					if (x.equals(y)){
						sb.append("1");
						sb.append(",");
						break;
					}
					if(y==goods[goods.length-1]){
						sb.append("0");
						sb.append(",");
					}
				}
			}
			sb.deleteCharAt(sb.length() - 1);
			sb.append(" ");
			sb.append(age);
			sb.append(" ");
			sb.append(gender);
			v.set(sb.toString());
			context.write(new Text(),v);
		}
	 }

	public static class Reduce extends Reducer<Text, Text, NullWritable, Text>{
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text x:values){
				context.write(NullWritable.get(),x);
			}
		}
	}
}
