package Final;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class userCF_job2 {
    public static  class  GoodsCooCurrenceListMapper extends Mapper<LongWritable, Text,Text,Text> {
        Text outkey = new Text();
        Text outvalue = new Text();
        // 10001  34,34,54,656
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] goods = value.toString().split("\t")[0].split(",");
            for (String a : goods) {
                for (String b : goods) {
                    outkey.set(a);
                    outvalue.set(b);
                    context.write(outkey,outvalue);
                }
            }
        }
    }

    public static  class GoodsCooCurrenceListReducer extends Reducer<Text,Text,Text,Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(key,value);
            }
        }
    }
}
