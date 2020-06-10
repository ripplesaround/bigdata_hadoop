package Final;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.net.URI;

import static Final.itemCF_job2.finalReducer.itemCF_write_in_file;
import static Final.userCF_job3.finalListReducer.userCF_write_in_file;

import static Final.userCF_job2.usercfjob2Listcombine.coefficient_age;
import static Final.userCF_job2.usercfjob2Listcombine.coefficient_gender;

public class job_control {
    public static int top_k = 2;   //选相似的k个物品,默认为2
    public job_control(){
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = (new GenericOptionsParser(conf, args)).getRemainingArgs();
        FileReader fr=new FileReader("config");
        BufferedReader br = new BufferedReader(fr);
        String str = br.readLine();
        otherArgs = str.split(" ");
        //可以写一个config来代替输入，更模块化
        top_k = Integer.valueOf(otherArgs[0]);
        coefficient_age = Double.valueOf(otherArgs[1]);
        coefficient_gender = Double.valueOf(otherArgs[2]);



        Path userCF_job1_in = new Path("recinput");
        Path userCF_job1_out = new Path("s1out");
        Path userCF_job2_out = new Path("s2out");
        Path userCF_job3_out = new Path("s3out");
        Path userCF_job_input = new Path("recinput");

        Path itemCF_job1_in = new Path("rechistory");
        Path itemCF_job1_out = new Path("j1out");
        Path itemCF_job2_in = new Path("recinput");
        Path itemCF_job2_out = new Path("j3out");

        // 删除(存在的)输出文件夹
        FileSystem fs = FileSystem.get(URI.create("recinput"), conf);
        if(fs.exists(itemCF_job1_out)){
            fs.delete(itemCF_job1_out, true);
        }
        if(fs.exists(itemCF_job2_out)){
            fs.delete(itemCF_job2_out, true);
        }
        if(fs.exists(userCF_job1_out)){
            fs.delete(userCF_job1_out , true);
        }
        if(fs.exists(userCF_job2_out)){
            fs.delete(userCF_job2_out , true);
        }
        if(fs.exists(userCF_job3_out)){
            fs.delete(userCF_job3_out , true);
        }




        Job userCF_job1 = Job.getInstance(conf,"userCF_job1");
        userCF_job1.setJarByClass(job_control.class);
        userCF_job1.setMapperClass(Final.userCF_job1.TokenListMapper.class);
        userCF_job1.setCombinerClass(Final.userCF_job1.ListCombiner.class);
        userCF_job1.setReducerClass(Final.userCF_job1.ListReducer_cal_sum.class);
        userCF_job1.setMapOutputKeyClass(Text.class);
        userCF_job1.setOutputValueClass(Text.class);
        userCF_job1.setOutputKeyClass(Text.class);
        userCF_job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(userCF_job1,userCF_job1_in);
        FileOutputFormat.setOutputPath(userCF_job1,userCF_job1_out);

        Job userCF_job2 = Job.getInstance(conf,"userCF_job2");
        userCF_job2.setJarByClass(job_control.class);
        userCF_job2.setMapperClass(Final.userCF_job2.TokensListMapper.class);
        userCF_job2.setCombinerClass(userCF_job2.usercfjob2Listcombine.class);
        userCF_job2.setReducerClass(Final.userCF_job2.usercfjob2ListReducer.class);
        userCF_job2.setOutputKeyClass(Text.class);
        userCF_job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(userCF_job2,userCF_job1_out);
        FileOutputFormat.setOutputPath(userCF_job2,userCF_job2_out);

        Job userCF_job3 = Job.getInstance(conf,"userCF_job3");
        userCF_job3.setJarByClass(job_control.class);
        userCF_job3.setMapperClass(userCF_job3.InputListMapper.class);
//        userCF_job3.setCombinerClass(userCF_job3.usercfjob2Listcombine.class);
        userCF_job3.setReducerClass(userCF_job3.finalListReducer.class);
        userCF_job3.setOutputKeyClass(Text.class);
        userCF_job3.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(userCF_job3,userCF_job_input);
        FileOutputFormat.setOutputPath(userCF_job3,userCF_job3_out);

        Job itemCF_job1 = Job.getInstance(conf, "itemCF_job1");
        itemCF_job1.setJarByClass(job_control.class);
        itemCF_job1.setMapperClass(itemCF_job1.TokenizerMapper.class);
        itemCF_job1.setCombinerClass(Final.itemCF_job1.Combine_toVector.class);
        itemCF_job1.setReducerClass(itemCF_job1.myReducer.class);
        itemCF_job1.setMapOutputKeyClass(Text.class);
        itemCF_job1.setMapOutputValueClass(Text.class);
        itemCF_job1.setOutputKeyClass(Text.class);
        itemCF_job1.setOutputValueClass(Text.class);
        //两个输入一样，省了复制的一步
        FileInputFormat.addInputPath(itemCF_job1, userCF_job1_in);
//        FileInputFormat.addInputPath(itemCF_job1, itemCF_job1_in);
        FileOutputFormat.setOutputPath(itemCF_job1, itemCF_job1_out);


        Job itemCF_job2 = Job.getInstance(conf, "itemCF_job2");
        itemCF_job2.setJarByClass(job_control.class);
        itemCF_job2.setMapperClass(itemCF_job2.InputMapper.class);
        itemCF_job2.setReducerClass(itemCF_job2.finalReducer.class);
        itemCF_job2.setMapOutputKeyClass(Text.class);
        itemCF_job2.setMapOutputValueClass(Text.class);
        itemCF_job2.setOutputKeyClass(Text.class);
        itemCF_job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(itemCF_job2, itemCF_job2_in);
        FileOutputFormat.setOutputPath(itemCF_job2, itemCF_job2_out);


        JobControl jobCtrl=new JobControl("myctrl");
        ControlledJob citemCF_job1 = new ControlledJob(conf);
        ControlledJob citemCF_job2 = new ControlledJob(conf);
        citemCF_job1.setJob(itemCF_job1);
        citemCF_job2.setJob(itemCF_job2);
        //依赖
        citemCF_job2.addDependingJob(citemCF_job1);

        ControlledJob cuserCF_job1 = new ControlledJob(conf);
        ControlledJob cuserCF_job2 = new ControlledJob(conf);
        ControlledJob cuserCF_job3 = new ControlledJob(conf);
        cuserCF_job1.setJob(userCF_job1);
        cuserCF_job2.setJob(userCF_job2);
        cuserCF_job3.setJob(userCF_job3);
        //依赖
        cuserCF_job2.addDependingJob(cuserCF_job1);
        cuserCF_job3.addDependingJob(cuserCF_job2);


        // 添加任务
        jobCtrl.addJob(citemCF_job1);
        jobCtrl.addJob(citemCF_job2);
        jobCtrl.addJob(cuserCF_job1);
        jobCtrl.addJob(cuserCF_job2);
        jobCtrl.addJob(cuserCF_job3);

        Thread jcThread = new Thread(jobCtrl);
        jcThread.start();
        while(true){
            if(jobCtrl.allFinished()){
                System.out.println(jobCtrl.getSuccessfulJobList());
                jobCtrl.stop();
                break;
            }
            if(jobCtrl.getFailedJobList().size() > 0){
                System.out.println(jobCtrl.getFailedJobList());
                jobCtrl.stop();
               break;
            }
        }

//        job1.waitForCompletion(true);
//        job2.waitForCompletion(true);




//         写入到本地方便sql操作
        String filename = "recommend_result_itemCF.txt";
        FileWriter writer;
        writer = new FileWriter(filename);
        writer.write(itemCF_write_in_file);
        writer.flush();
        writer.close();
        System.out.println("基于itemCF的推荐结果写入 recommend_result_itemCF 文件");
        filename = "recommend_result_userCF.txt";
        writer = new FileWriter(filename);
        writer.write(userCF_write_in_file);
        writer.flush();
        writer.close();
        System.out.println("基于userCF的推荐结果写入 recommend_result_userCF 文件");
    }

}
