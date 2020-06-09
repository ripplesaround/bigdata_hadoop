package Final;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.security.SaslOutputStream;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.StringTokenizer;

import static Final.s1.TokenizerMapper.item_size;
import static Final.s1.TokenizerMapper.user_size;
import static Final.s1.myReducer.history;

// idea
// 第一个任务：相似度矩阵

public class s1  {
    static private int top_k = 3;   //选相似的k个物品


    public s1(){
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = (new GenericOptionsParser(conf, args)).getRemainingArgs();

        if (otherArgs.length < 2) {
            System.err.println("Usage: <in> [<in>...] <out>");
            System.exit(2);
        }

        // 删除输出文件夹
        FileSystem fs = FileSystem.get(URI.create(args[0]), conf);
        fs.delete(new Path(otherArgs[1]), true);
        fs.delete(new Path(otherArgs[3]), true);

        Job job1 = Job.getInstance(conf, "s1");
        job1.setJarByClass(s1.class);

        job1.setMapperClass(s1.TokenizerMapper.class);
        job1.setCombinerClass(s1.IntSumReducer_com.class);
        job1.setReducerClass(s1.myReducer.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
//        for (int i = 0; i < otherArgs.length - 1; ++i) {
//            FileInputFormat.addInputPath(job1, new Path(otherArgs[i]));
//        }
        FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]));
        job1.waitForCompletion(true);

        Job job2 = Job.getInstance(conf, "s2");
//        job2.setJarByClass(s1.class);

        job2.setMapperClass(s1.InputMapper.class);
        job2.setReducerClass(s1.finalReducer.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path(otherArgs[2]));
        FileOutputFormat.setOutputPath(job2, new Path(otherArgs[3]));
        job2.waitForCompletion(true);
    }
    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
        // age gender 与itemcf无关
        public static int user_size = 0;
        public static int item_size = 0;
        private int cnt_temp =0;
        private Text word = new Text();
        public TokenizerMapper() {
        }
        public void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            String[] s_temp =  value.toString().split(" " );
            if(Character.isAlphabetic(s_temp[0].toCharArray()[0])){
                // 第一行不扫描
                return;
            }
            String[] s = s_temp[1].split(",");
            item_size = s.length;
            int temp1 = 1;

            for(String buy_cnt1:s){
                int temp2 = 1;
                for(String buy_cnt2:s) {
                    this.word.set("item" + temp1+"\titem"+temp2);
                    if (buy_cnt1.equals("1")) {
                        if (buy_cnt2.equals("1")) {
                            context.write(this.word, new Text( "1\t1"));
                        } else {
                            context.write(this.word, new Text( "1\t0"));
                        }
                    } else {
                        if (buy_cnt2.equals("1")) {
                            context.write(this.word, new Text( "0\t1"));
                        } else {
                            context.write(this.word, new Text( "0\t0"));
                        }
                    }
                    temp2++;
                }
                temp1++;
            }
            cnt_temp++;
            user_size = cnt_temp;
        }
    }

    public static class IntSumReducer_com extends Reducer<Text, Text, Text, Text> {
        // 构造向量乘法
        private Text result = new Text();
        public IntSumReducer_com() {
        }
        public void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            int sum = 0;
            Text val = new Text();
            String ans = "";
            int i_norm=0,j_norm=0;  //范数
            int product = 0;
            for(Iterator i$ = values.iterator(); i$.hasNext(); ) {
                val = (Text)i$.next();
                String val_str = val.toString();
                String[] s = val_str.split("\t");//user1\t1
                int temp_i = Integer.valueOf(s[0]);
                int temp_j = Integer.valueOf(s[1]);
                i_norm += temp_i*temp_i;
                j_norm += temp_j*temp_j;
                product += temp_i*temp_j;
            }
            double cos_sim = product/(Math.sqrt(j_norm)*Math.sqrt(i_norm));
            String[] name = key.toString().split("\t");
            if(!name[0].equals(name[1])){
                this.result.set(name[1] +"\t"+String.valueOf(cos_sim));
                context.write(new Text(name[0]), this.result);
            }
            // 这里就计算完了两个物品间的相似度，然后可以写入相似度矩阵，也可以记录每个物品和谁相似，方便后续处理
        }
    }

    public static class myReducer extends Reducer<Text, Text, Text, Text> {
        public static String history = "";

        public myReducer() {
        }
        public void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {

            Text val = null;
            double max_con_sim[] = new double[top_k];
            String max_index[] = new String[top_k];
            for(int i=0;i<top_k;++i){
                max_con_sim[i] = 0.0;
                max_index[i] = "$noone";
            }
//            int temp = 0;
            for(Iterator i$ = values.iterator(); i$.hasNext(); ) {
//                context.write( key,(Text) i$.next());
//                temp++;
                val = (Text) i$.next();
                String[] s = val.toString().split("\t");
                double cos_sim = Double.valueOf(s[1]);
                String item_name = s[0];
                for(int i=0;i<top_k;++i){
                    if(cos_sim>=max_con_sim[i]){
                        for(int j=top_k-1;j>i;--j){  //后推
                            max_con_sim[j] = max_con_sim[j-1];
                            max_index[j] = max_index[j-1];
                        }
                        max_con_sim[i] = cos_sim;
                        max_index[i] = item_name;
                        i+=top_k;
//                        break;
                    }
                }
            }
//            if(temp<2){
//                return;
//            }
            String ans = "";
            for(int i=0;i<top_k;++i){
                ans+= max_index[i];
                ans+="\t";
                ans+=String.valueOf(max_con_sim[i]);
                if(i!=top_k-1){
                    ans+="$$";
                }
            }
            context.write( key,new Text(ans));
            history += (key.toString()+"$$"+ans+"\n");
        }
    }

    public static class InputMapper extends Mapper<Object, Text, Text, Text> {

        public static double recommend_table[] = new double[item_size];
        String[] history_table = history.split("\n");
        public InputMapper() {
        }
        public void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            String[] s =  value.toString().split("," );
            for(int i=0;i<item_size;++i){
                recommend_table[i] = 0.0;
            }
            for(int i=0;i<item_size;++i){

                if(s[i].equals("1")){
                    String[] temp = history_table[i].split("\\$\\$");//$是转意字符
//                    context.write(new Text(Integer.toString(temp.length)),new Text("index"));
                    for(int j=top_k;j>0;--j){  //注意是反过来的
                        String[] val = temp[j].split("\t");
                        String index = val[0].substring(4);
//                        context.write(new Text(Integer.toString(temp.length)),new Text(index));
                        recommend_table[Integer.valueOf(index)-1] += Double.valueOf(val[1]);
                    }

                }
                else {
                    //0 表示用户没有和该商品互动过
                }
            }
            double max_rec[] = new double[top_k];
            String max_index[] = new String[top_k];
            for(int i=0;i<top_k;++i){
                max_rec[i] = 0.0;
                max_index[i] = "$noone";
            }
            for(int cnt=0;cnt<item_size;++cnt){
                for(int i=0;i<top_k;++i){
                    if(recommend_table[cnt]>=max_rec[i]){
                        for(int j=top_k-1;j>i;--j){  //后推
                            max_rec[j] = max_rec[j-1];
                            max_index[j] = max_index[j-1];
                        }
                        max_rec[i] = recommend_table[cnt];
                        max_index[i] = "item"+(cnt+1);
                        i+=top_k;
    //                        break;
                    }
                }
            }
            for(int i=0;i<top_k;++i){
                context.write(new Text(max_index[i]),new Text(Double.toString(max_rec[i])));
            }
        }
    }
    public static class finalReducer extends Reducer<Text, Text, Text, Text> {
        public finalReducer() {
        }
        public void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {

            Text val = null;

            for(Iterator i$ = values.iterator(); i$.hasNext(); ) {
                context.write( key,(Text) i$.next());
            }
        }
    }
}
