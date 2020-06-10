package Final;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static Final.itemCF_job1.TokenizerMapper.item_size;
import static Final.itemCF_job1.myReducer.itemCF_history;
import static Final.itemCF_job1.myReducer.itemCF_history_index;
import static Final.job_control.top_k;

public class itemCF_job2 {
    public static class InputMapper extends Mapper<Object, Text, Text, Text> {

        public static double recommend_table[] = new double[item_size];
        public static String[] item_ref = new String[item_size];
        String[] history_table = itemCF_history.split("\n");
        String[] history_table_index_temp = itemCF_history_index.split("\n");
        List<String> history_table_index = Arrays.asList(history_table_index_temp);
        public InputMapper() {
        }
        @Override
        public void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            String[] s_temp = value.toString().split(" ");
            if(Character.isAlphabetic(s_temp[0].toCharArray()[0])){
                // 第一行扫描名称
                // 保证名称和序号对应 11->2
                String[] s1=s_temp[1].split("\\(|\\)");
                String[] s2 = s1[1].split(",");
                for(int i=0;i<s2.length;++i){
                    item_ref[i] = "item"+s2[i];
                }
                return;
            }
            String current_user = "user"+ s_temp[0];
            String[] s =  s_temp[1].toString().split("," );
            for(int i=0;i<item_size;++i){
                recommend_table[i] = 0.0;
            }
            //这个 假设i->i
            for(int i=0;i<item_size;++i){
                if(s[i].equals("1")){
                    //数据中是没有userid的所以假设递增
                    String[] temp = history_table[i].split("\\$\\$");//$是转意字符
//                    context.write(new Text(Integer.toString(temp.length)),new Text("index"));
                    for(int j=top_k;j>0;--j){  //注意是反过来的
                        String[] val = temp[j].split("\t");
                        String index = val[0].substring(4);
//                        context.write(new Text(Integer.toString(temp.length)),new Text(index));
                        recommend_table[Integer.valueOf(index)-1] += Double.valueOf(val[1]);
                    }
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
            //输出格式
//            user1   item7   2.9596747752497685
//            user1   item1   2.4944271909999154
//            user1   item3   2.9652475842498527
//            for(int i=0;i<top_k;++i){
//                context.write(new Text(current_user),new Text(max_index[i]+"\t"+Double.toString(max_rec[i])));
//            }
            String result = "";
            for(int i=0;i<top_k;++i){
                // 最后要换成初始的label
                int index = Integer.valueOf(max_index[i].substring(4));
                result+= item_ref[index];
                if(i!=top_k-1)
                    result+=",";
            }
            context.write(new Text(current_user),new Text(result));
        }
    }

    public static class finalReducer extends Reducer<Text, Text, Text, Text> {
        public static String itemCF_write_in_file = "";
        public finalReducer() {
        }
        @Override
        public void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            Text val = null;
            for(Iterator i$ = values.iterator(); i$.hasNext(); ) {
                Text temp = (Text) i$.next();
                itemCF_write_in_file += (key.toString()+","+temp.toString()+"\n");
                context.write( key,temp);
            }
        }
    }
}
