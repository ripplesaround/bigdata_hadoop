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

import static Final.itemCF_job2.finalReducer.write_in_file;


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
        str = br.readLine();
        top_k = Integer.valueOf(str);
        //可以写一个config来代替输入，更模块化


        Path userCF_job1_in = new Path("input");
        Path userCF_job1_out = new Path("s1out");
        Path userCF_job2_out = new Path("s2out");

        // 删除(存在的)输出文件夹
        FileSystem fs = FileSystem.get(URI.create(otherArgs[0]), conf);
        if(fs.exists(new Path(otherArgs[1]))){
            fs.delete(new Path(otherArgs[1]), true);
        }
        if(fs.exists(new Path(otherArgs[3]))){
            fs.delete(new Path(otherArgs[3]), true);
        }
        if(fs.exists(userCF_job1_out)){
            fs.delete(userCF_job1_out , true);
        }
        if(fs.exists(userCF_job2_out)){
            fs.delete(userCF_job2_out , true);
        }




        Job userCF_job1 = Job.getInstance(conf,"userCF_job1");
        userCF_job1.setJarByClass(job_control.class);
        userCF_job1.setMapperClass(Final.userCF_job1.TokenListMapper.class);
        userCF_job1.setCombinerClass(userCF_job1.ListCombiner.class);
        userCF_job1.setReducerClass(Final.userCF_job1.ListReducer_cal_sum.class);
        userCF_job1.setMapOutputKeyClass(Text.class);
        userCF_job1.setOutputValueClass(Text.class);
        userCF_job1.setOutputKeyClass(Text.class);
        userCF_job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(userCF_job1,userCF_job1_in);
        FileOutputFormat.setOutputPath(userCF_job1,userCF_job1_out);


        Job userCF_job2 = Job.getInstance(conf,"userCF_job2");
        userCF_job2.setJarByClass(job_control.class);
        userCF_job2.setMapperClass(userCF_job2.GoodsCooCurrenceListMapper.class);
        userCF_job2.setReducerClass(userCF_job2.GoodsCooCurrenceListReducer.class);
        userCF_job2.setOutputKeyClass(Text.class);
        userCF_job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(userCF_job2,userCF_job1_in);
        FileOutputFormat.setOutputPath(userCF_job2,userCF_job2_out);

        Job itemCF_job1 = Job.getInstance(conf, "itemCF_job1");
        itemCF_job1.setJarByClass(job_control.class);
        itemCF_job1.setMapperClass(itemCF_job1.TokenizerMapper.class);
        itemCF_job1.setCombinerClass(Final.itemCF_job1.Combine_toVector.class);
        itemCF_job1.setReducerClass(itemCF_job1.myReducer.class);
        itemCF_job1.setMapOutputKeyClass(Text.class);
        itemCF_job1.setMapOutputValueClass(Text.class);
        itemCF_job1.setOutputKeyClass(Text.class);
        itemCF_job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(itemCF_job1, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(itemCF_job1, new Path(otherArgs[1]));


        Job itemCF_job2 = Job.getInstance(conf, "itemCF_job2");
        itemCF_job2.setJarByClass(job_control.class);
        itemCF_job2.setMapperClass(itemCF_job2.InputMapper.class);
        itemCF_job2.setReducerClass(itemCF_job2.finalReducer.class);
        itemCF_job2.setMapOutputKeyClass(Text.class);
        itemCF_job2.setMapOutputValueClass(Text.class);
        itemCF_job2.setOutputKeyClass(Text.class);
        itemCF_job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(itemCF_job2, new Path(otherArgs[2]));
        FileOutputFormat.setOutputPath(itemCF_job2, new Path(otherArgs[3]));


        JobControl jobCtrl=new JobControl("myctrl");
        ControlledJob citemCF_job1 = new ControlledJob(conf);
        ControlledJob citemCF_job2 = new ControlledJob(conf);
        citemCF_job1.setJob(itemCF_job1);
        citemCF_job2.setJob(itemCF_job2);

        citemCF_job2.addDependingJob(citemCF_job1);

        ControlledJob cuserCF_job1 = new ControlledJob(conf);
        ControlledJob cuserCF_job2 = new ControlledJob(conf);
//        ControlledJob cuserCF_job3 = new ControlledJob(conf);
//        ControlledJob cuserCF_job4 = new ControlledJob(conf);
//        ControlledJob cuserCF_job5 = new ControlledJob(conf);
//        ControlledJob cuserCF_job6 = new ControlledJob(conf);
        cuserCF_job1.setJob(userCF_job1);
//        cuserCF_job2.setJob(userCF_job2);
//        cuserCF_job2.setJob(userCF_job2);
//        cuserCF_job2.setJob(userCF_job2);
//        cuserCF_job2.setJob(userCF_job2);
//        cuserCF_job2.setJob(userCF_job2);

//        cuserCF_job2.addDependingJob(cuserCF_job1);

        jobCtrl.addJob(citemCF_job1);
        jobCtrl.addJob(citemCF_job2);
        jobCtrl.addJob(cuserCF_job1);
//        jobCtrl.addJob(cuserCF_job2);

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




        // 写入到本地方便sql操作
        String filename = "recommend_result_itemCF.txt";
        FileWriter writer;
        writer = new FileWriter(filename);
        writer.write(write_in_file);
        writer.flush();
        writer.close();
        System.out.println("基于itemCF的推荐结果写入 recommend_result_itemCF 文件");
    }

}
