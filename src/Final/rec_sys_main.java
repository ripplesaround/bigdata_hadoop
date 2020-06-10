package Final;

import Final.DataClean.Entry;
import Final.rec_alg.job_control;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
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
        int rec_or_not = 0;
        while (true){
            System.out.println("——————————————");
            System.out.println("\t可选操作");
            System.out.println("put\t\t\t\t：上传数据集");
            System.out.println("data_preprocess\t：执行数据预处理");
            System.out.println("config\t\t\t：修改超参数");
            System.out.println("recommend_alg\t：运行推荐算法");
            System.out.println("result\t\t\t：查看推荐结果");
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
                rec_or_not  = 1;
            }
            else if(input_op.equals("clean")){
                rec_or_not = 0;
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
            else if(input_op.equals("result")){
                if (rec_or_not==0){
                    System.out.println("未执行推荐算法，请重试");
                }
                else{
                    String filename1 = "recommend_result_itemCF.txt";
                    String filename2 = "recommend_result_userCF.txt";
                    System.out.println("———————————");
                    System.out.println("itemCF的结果");
                    FileReader fr=new FileReader(filename1);
                    BufferedReader br = new BufferedReader(fr);
                    String str;
                    while((str = br.readLine())!=null){
                        System.out.println(str);
                    }
                    System.out.println("———————————");
                    System.out.println("userCF的结果");
                    FileReader fr1=new FileReader(filename2);
                    BufferedReader br1 = new BufferedReader(fr1);
                    String str1;
                    while((str1 = br1.readLine())!=null){
                        System.out.println(str1);
                    }
                    fr.close();
                    fr1.close();
                }
                System.out.println("——————————————");
            }
            else if(input_op.equals("config")){
                String filename = "config";
                FileWriter writer;

                FileReader fr=new FileReader(filename);
                BufferedReader br = new BufferedReader(fr);
                String temp1 = br.readLine();
                String[] name = br.readLine().split(" ");
                fr.close();
                writer = new FileWriter(filename);
                Scanner input_config = new Scanner(System.in);
                String str = null;

//                name[0] = "top_k";
//                name[1] = "年龄比重";
//                name[2] = "性别比重";

                String write_infile = "";
                for(int i=0;i<3;++i){
                    System.out.println("请输入"+name[i]+":");
                    str = input_config.next();
                    write_infile +=(str+" ");
                }
                write_infile+="\n";
                writer.write(write_infile);
                writer.append(name[0]+" "+name[1]+" "+name[2]);
                writer.close();
                System.out.println("修改完成");
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
