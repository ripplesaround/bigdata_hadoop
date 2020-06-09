package Final;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.util.Iterator;

import static Final.itemCF_job2.finalReducer.write_in_file;

// idea
// 第一个任务：相似度矩阵

public class job_control {
    public static int top_k = 3;   //选相似的k个物品
    public job_control(){
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = (new GenericOptionsParser(conf, args)).getRemainingArgs();
        FileReader fr=new FileReader("config");
        BufferedReader br = new BufferedReader(fr);
        String str = br.readLine();
        otherArgs = str.split(" ");
        //可以写一个config来代替输入
//        if (otherArgs.length < 2) {
//            FileReader fr=new FileReader("config");
//            BufferedReader br = new BufferedReader(fr);
//            String str = br.readLine();
//            otherArgs = str.split(" ");
////            System.err.println("Usage: <in> [<in>...] <out>");
////            System.exit(2);
//        }

        // 删除输出文件夹
        //arg[0]
//        System.out.println(otherArgs[0]);
        FileSystem fs = FileSystem.get(URI.create(otherArgs[0]), conf);
        if(fs.exists(new Path(otherArgs[1]))){
            fs.delete(new Path(otherArgs[1]), true);
        }
        if(fs.exists(new Path(otherArgs[3]))){
            fs.delete(new Path(otherArgs[3]), true);
        }

        Job job1 = Job.getInstance(conf, "s1");
        job1.setJarByClass(job_control.class);
        job1.setMapperClass(itemCF_job1.TokenizerMapper.class);
        job1.setCombinerClass(itemCF_job1.IntSumReducer_com.class);
        job1.setReducerClass(itemCF_job1.myReducer.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
//        for (int i = 0; i < otherArgs.length - 1; ++i) {
//            FileInputFormat.addInputPath(job1, new Path(otherArgs[i]));
//        }
        FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]));


        Job job2 = Job.getInstance(conf, "s2");
//        job2.setJarByClass(s1.class);

        job2.setMapperClass(itemCF_job2.InputMapper.class);
        job2.setReducerClass(itemCF_job2.finalReducer.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path(otherArgs[2]));
        FileOutputFormat.setOutputPath(job2, new Path(otherArgs[3]));


        JobControl jobCtrl=new JobControl("myctrl");
        ControlledJob cjob1 = new ControlledJob(conf);
        ControlledJob cjob2 = new ControlledJob(conf);
        cjob1.setJob(job1);
        cjob2.setJob(job2);

        cjob2.addDependingJob(cjob1);

        jobCtrl.addJob(cjob1);
        jobCtrl.addJob(cjob2);

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
