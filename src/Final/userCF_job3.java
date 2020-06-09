package Final;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static Final.itemCF_job1.TokenizerMapper.item_size;
import static Final.job_control.top_k;
import static Final.userCF_job1.TokenListMapper.item_num;
import static Final.userCF_job2.usercfjob2ListReducer.userCF_history;
import static Final.userCF_job2.usercfjob2ListReducer.userCF_history_index;

public class userCF_job3 {
    public static class InputListMapper extends Mapper<Object, Text, Text, Text> {
        public static double recommend_table[] = new double[item_num];
        public static double recommend_table_frac[] = new double[item_num];
        String[] history_table = userCF_history.split("\n");
        String[] history_table_index_temp = userCF_history_index.split("\n");
        List<String> history_table_index = Arrays.asList(history_table_index_temp);
        public InputListMapper() {
        }
        @Override
        public void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            String[] s_temp = value.toString().split(" ");
            if(Character.isAlphabetic(s_temp[0].toCharArray()[0])){
                // 第一行不扫描
                return;
            }
            String current_user = "user"+ s_temp[0];
            //拿到自己的对应的sim user，实际上应该是洗数据排序的工作
            int current_user_index = history_table_index.indexOf(current_user);
            String[] sim_user = history_table[current_user_index].split("\\$\\$");

            for(int i=0;i<item_num;++i){
                recommend_table[i] = 0.0;
                recommend_table_frac[i] = 0.0;
            }
            for(int j=1;j<=top_k;++j){
                String[] s = sim_user[j].split("\t");
                double rec_num = Double.valueOf(s[1]);
                String[] user_history = s[2].split(",");
                for(int i=0;i<item_num;++i){
                    recommend_table[i] += rec_num*Double.valueOf(user_history[i]);
                    recommend_table_frac[i] += rec_num;
                }
            }
            for(int i=0;i<item_num;++i){
                recommend_table[i] /= recommend_table_frac[i];
//                context.write(new Text(String.valueOf(i)),new Text(String.valueOf(recommend_table[i])));
            }

            double max_rec[] = new double[top_k];
            String max_index[] = new String[top_k];
            for(int i=0;i<top_k;++i){
                max_rec[i] = 0.0;
                max_index[i] = "$noone";
            }
            for(int cnt=0;cnt<item_num;++cnt){
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
//                    else{
//                        context.write(new Text(current_user+" "+String.valueOf(recommend_table[cnt])),new Text(String.valueOf(max_rec[i])));
//                    }
                }
            }
            String result = "";
            for(int i=0;i<top_k;++i){
                result+=max_index[i];
                if(i!=top_k-1)
                    result+=",";
            }
            context.write(new Text(current_user),new Text(result));
        }
    }

    public static class finalListReducer extends Reducer<Text, Text, Text, Text> {
        public static String userCF_write_in_file = "";
        public finalListReducer() {
        }
        @Override
        public void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            Text val = null;
            for(Iterator i$ = values.iterator(); i$.hasNext(); ) {
                Text temp = (Text) i$.next();
                userCF_write_in_file += (key.toString()+","+temp.toString()+"\n");
                context.write( key,temp);
            }
        }
    }
}
