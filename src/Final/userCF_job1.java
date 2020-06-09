package Final;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static Final.userCF_job1.TokenListMapper.user_id;

public class userCF_job1 {

    public static class TokenListMapper extends Mapper<LongWritable, Text, Text, Text> {
        public static int user_num = 0;
        public static int item_num = 0;
        public static int propert_num = 0;
        public static List<String> user_id = new ArrayList<String>();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] s_temp =  value.toString().split(" " );
            if(Character.isAlphabetic(s_temp[0].toCharArray()[0])){
                // 第一行不扫描
                return;
            }
            if(propert_num==0){
                propert_num = s_temp.length -2;
            }
            String val = "";
            for(int i=1;i<s_temp.length;++i){
                val+=s_temp[i];
                if(i!=s_temp.length-1){
                    val+="\t";
                }
            }
            user_id.add("user"+s_temp[0]);
            context.write(new Text("user"+s_temp[0]),new Text(val));
            user_num++;
        }
    }
    public static class ListCombiner extends Reducer<Text, Text, Text, Text> {
        // 构造向量乘法
        Text goodList = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Text val = new Text();
            for(Iterator i$ = values.iterator(); i$.hasNext(); ){
                val = (Text)i$.next();
                for(String temp:user_id){
                    context.write(new Text(key.toString()+"\t"+temp), val);
                    context.write(new Text(temp+"\t"+key.toString()), val);
                }
            }
        }
    }

    public static class ListReducer_cal_sum extends Reducer<Text, Text, Text, Text> {
        //计算两个user之间的相似度
        Text goodList = new Text();
        // 系数
        public static double coefficient_age = 0.05;
        public static double coefficient_gender = 1.0;
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Text val = new Text();
            // 这里根据 属性 的变化需要修改
            int user1_age=0,user2_age=0;
            int user1_gender=-1,user2_gender=-1;
            String user1_history=null,user2_history=null;
            for(Iterator i$ = values.iterator(); i$.hasNext(); ){
                val = (Text)i$.next();
                String[] s = val.toString().split("\t");
                if(user1_age==0){
                    user1_age = Integer.valueOf(s[1]);
                }else{
                    user2_age = Integer.valueOf(s[1]);
                }
                if(user1_gender==1){
                    user1_gender = Integer.valueOf(s[2]);
                }else{
                    user2_gender = Integer.valueOf(s[2]);
                }
                if(user1_history.equals(null)){
                    user1_history = s[0];
                }else{
                    user2_history = s[0];
                }
            }
            String[] user1_temp = user1_history.split(",");
            String[] user2_temp = user2_history.split(",");
            int i_norm=0,j_norm=0;  //范数
            int product = 0;
            for(int i=0;i<user1_temp.length;++i){
                product += (Integer.valueOf(user1_temp[i])*Integer.valueOf(user2_temp[i]));
                i_norm += (Integer.valueOf(user1_temp[i])*Integer.valueOf(user1_temp[i]));
                j_norm += (Integer.valueOf(user2_temp[i])*Integer.valueOf(user2_temp[i])) ;
            }
            double cos_sim_history = product/(Math.sqrt(j_norm)*Math.sqrt(i_norm));
            // 计算属性相似度
            // 越想近越相似值越大
            double con_sim_age = (Math.abs(user1_age-user2_age));
            double con_sim_gender = (Math.abs(user1_gender-user2_gender));

            double cos_sim = cos_sim_history
                    + coefficient_age*(Math.abs(user1_age-user2_age))
                    + coefficient_gender*(Math.abs(user1_gender-user2_gender));


            context.write(key, val);
        }
    }
}
