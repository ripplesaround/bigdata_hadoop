package Final;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

import static Final.job_control.top_k;
import static Final.userCF_job1.TokenListMapper.item_num;

public class userCF_job2 {
    public static  class TokensListMapper extends Mapper<LongWritable, Text,Text,Text> {
        Text outkey = new Text();
        Text outvalue = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] s = value.toString().split(" ");

            String user_name1 = s[0];
            String user_name2 = s[1];
//            s[2]+"\t"+s[3]+"\t"+s[4]
            context.write(new Text(user_name1.substring(8)+"\t"+user_name2),new Text(s[2]+"\t"+s[3]+"\t"+s[4]));
        }
    }

    public static  class usercfjob2Listcombine extends Reducer<Text,Text,Text,Text> {
        public static double coefficient_age = 0.05;
        public static double coefficient_gender = 1.0;
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int user1_age=0,user2_age=0;
            int user1_gender=-1,user2_gender=-1;
            String user1_history="test",user2_history="test";
            String[] s_name = key.toString().split("\t");
            String user1_name = s_name[0],user2_name = s_name[1];
            for (Text val : values) {
                String[] s = val.toString().split("\t");
                if(user1_age==0){
                    user1_age = Integer.valueOf(s[1]);
                }else{
                    user2_age = Integer.valueOf(s[1]);
                }
                if(user1_gender==-1){
                    user1_gender = Integer.valueOf(s[2]);
                }else{
                    user2_gender = Integer.valueOf(s[2]);
                }
                if(user1_history.equals("test")){
                    user1_history = s[0];
                }else{
                    user2_history = s[0];
                }
            }
            String[] user1_temp = user1_history.split(",");
            String[] user2_temp = user2_history.split(",");
            int i_norm=0,j_norm=0;  //范数
            int product = 0;
            item_num = user1_temp.length;
            for(int i=0;i<user1_temp.length;++i){
                product += (Integer.valueOf(user1_temp[i])*Integer.valueOf(user2_temp[i]));
                i_norm += (Integer.valueOf(user1_temp[i])*Integer.valueOf(user1_temp[i]));
                j_norm += (Integer.valueOf(user2_temp[i])*Integer.valueOf(user2_temp[i])) ;
            }
            double cos_sim_history = product/(Math.sqrt(j_norm)*Math.sqrt(i_norm));
            // 计算属性相似度
            // 越想近越相似值越大
            double cos_sim_age = Math.cos((Math.abs(user1_age-user2_age))
                                    /(Math.max(user1_age,user2_age)));
            double cos_sim_gender =  Math.cos(Math.abs(user1_gender-user2_gender));

            double cos_sim = cos_sim_history
                    + coefficient_age * cos_sim_age
                    + coefficient_gender * cos_sim_gender;


            // 要记录对方的行为模式
            context.write(new Text(user1_name), new Text(user2_name+"\t"+String.valueOf(cos_sim)+"\t"+user2_history));
        }
    }
    public static  class usercfjob2ListReducer extends Reducer<Text,Text,Text,Text> {
        public static String userCF_history = "";
        public static String userCF_history_index = "";
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double max_con_sim[] = new double[top_k];
            String max_index[] = new String[top_k];
            String max_index_history[] = new String[top_k];
            for(int i=0;i<top_k;++i){
                max_con_sim[i] = 0.0;
                max_index[i] = "$noone";
                max_index_history[i] = "$nothing";
            }
            for (Text val : values) {
                String[] s = val.toString().split("\t");
                double cos_sim = Double.valueOf(s[1]);
                String user_name = s[0];
                String user_history = s[2];
                //物品可以和自己相似，用户不能和自己相似
                if(user_name.equals(key.toString())){
                    continue;
                }
                for(int i=0;i<top_k;++i){
                    if(cos_sim>=max_con_sim[i]){
                        for(int j=top_k-1;j>i;--j){  //后推
                            max_con_sim[j] = max_con_sim[j-1];
                            max_index[j] = max_index[j-1];
                            max_index_history[j] = max_index_history[j-1];
                        }
                        max_con_sim[i] = cos_sim;
                        max_index[i] = user_name;
                        max_index_history[i] = user_history;
                        i+=top_k;
//                        break;
                    }
                }
            }
            String ans = "";
            for(int i=0;i<top_k;++i){
                ans+= max_index[i];
                ans+="\t";
                ans+=String.valueOf(max_con_sim[i]);
                ans+="\t";
                ans+=max_index_history[i];
                if(i!=top_k-1){
                    ans+="$$";
                }
            }
            context.write( key,new Text(ans));
            userCF_history += (key.toString()+"$$"+ans+"\n");
            userCF_history_index += (key.toString()+"\n");
        }
    }
}
