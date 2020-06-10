package Final.rec_alg;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;
import static Final.rec_alg.job_control.top_k;

public class itemCF_job1 {
    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
        // age gender 与itemcf无关
        public static int user_size = 0;
        public static int item_size = 0;
        private int cnt_temp =0;
        private Text word = new Text();
        public TokenizerMapper() {
        }
        @Override
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

    public static class Combine_toVector extends Reducer<Text, Text, Text, Text> {
        // 构造向量乘法
        private Text result = new Text();
        public Combine_toVector() {
        }
        @Override
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
        public static String itemCF_history = "";
        public static String itemCF_history_index = "";
        public myReducer() {
        }
        @Override
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
            itemCF_history += (key.toString()+"$$"+ans+"\n");
            itemCF_history_index += (key.toString()+"\n");
        }
    }
}
