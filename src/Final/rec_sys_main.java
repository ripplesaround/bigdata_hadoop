package Final;

import Final.DataClean.Entry;
import Final.rec_alg.job_control;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;
import java.sql.SQLOutput;
import java.util.Scanner;

public class rec_sys_main {
    public static void main(String[] args) throws Exception {
        System.out.println("————————————————————————————");
        System.out.println("欢迎使用本推荐系统");
        String input_op = "init";
        Scanner input= new Scanner(System.in);
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create("recinput"), conf);
        while (true){
            System.out.println("——————————————");
            System.out.println("\t可选操作");
            System.out.println("put\t\t\t\t：上传数据集");
            System.out.println("data_preprocess\t：执行数据预处理");
            System.out.println("recommend_alg\t：运行推荐算法并输出");
            System.out.println("clean\t\t\t：初始化");
            System.out.println("请输入您的操作：");

            input_op = input.next();
            if(input_op.equals("put")){
                Path dst_path = new Path("input/file0");
                Path src_path = new Path("other/3000_test");
                fs.copyFromLocalFile(src_path,dst_path);
                System.out.println("上传成功");
                System.out.println("——————————————");
            }
            else if(input_op.equals("data_preprocess")){
                String[] entry_args = new String[2];
                entry_args[0] = "input/file0";
                entry_args[1] = "recinput/test4";
                Entry.main(entry_args);
                System.out.println("数据预处理成功");
                System.out.println("——————————————");
            }
            else if(input_op.equals("recommend_alg")){
                String[] job_control_args = null;
                job_control.main(job_control_args);
//                System.out.println("上传成功")
                System.out.println("——————————————");
            }
            else if(input_op.equals("clean")){
                Path mid_clean1 = new Path("recinput");
                Path mid_clean2 = new Path("input/file0");
                Path mid_clean3 = new Path("rechistory");   //暂时不清理

                if(fs.exists(mid_clean1)){
                    fs.delete(mid_clean1, true);
                }
                if(fs.exists(mid_clean2)){
                    fs.delete(mid_clean2, true);
                }

                fs.mkdirs(mid_clean1);
                fs.mkdirs(mid_clean2);
                System.out.println("初始化成功");
                System.out.println("——————————————");
            }
            else if(input_op.equals("exit")){

                System.out.println("\tbye :)");
                System.out.println("————————————————————————————");
                break;
            }
            else{
                System.out.println("不识别输入的命令，请重新输入!");
                System.out.println("——————————————");
            }

        }
    }
}
