package com.company;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class Main {
    public static void main(String[] args) throws IOException, URISyntaxException {
//        upload();
        download();
    }
    public static void test(String[] args) {
        try {
            String filename = "hdfs://localhost:9000/user/hadoop/test/test.txt";
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);
            if(fs.exists(new Path(filename))){
                System.out.println("文件存在");
            }else{
                System.out.println("文件不存在");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static byte[] input2bytearray(String filepath) throws IOException {
        InputStream in = new FileInputStream(filepath);
        byte[] data = tobytearray(in);
        in.close();
        return data;
    }

    private static byte[] tobytearray(InputStream in) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024*4];
        int n = 0;
        while((n = in.read(buffer))!=-1){
            out.write(buffer,0,n);
        }
        return out.toByteArray();
    }
    public static void upload() throws IOException, URISyntaxException {
//        name = "test.txt"
//        这里默认在test文件夹里处理相关文件
        String hdfs = "hdfs://localhost:9000";
        String filename = "test.txt";
        String dst_path = "/user/hadoop/test/";
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI(hdfs),conf);
        if(fs.exists(new Path(hdfs+dst_path+filename))){
            System.out.println("文件存在");
            byte[] buff = input2bytearray(filename);
            FSDataOutputStream os = fs.append(new Path(dst_path+filename));
            os.write(buff,0,buff.length);
            os.close();
        }
        else{
            System.out.println("文件不存在");
            fs.copyFromLocalFile(new Path(filename),new Path(dst_path));
        }
        fs.close();
        System.out.println("upload done");
    }
    public static void download() throws URISyntaxException, IOException {
        String hdfs = "hdfs://localhost:9000";
        String src_path = "/user/hadoop/test/";
        String filename = "test.txt";
        Configuration conf = new Configuration();
        File file = new File(filename);
        FileSystem fs = FileSystem.get(new URI(hdfs),conf);
        if(file.exists()) {
            System.out.println("本地已经存在");
            String[] temp = filename.split("\\.");
            System.out.println(temp[0]);
            String filename_new = temp[0] + "(1)." + temp[1];
            System.out.println(filename_new);
            fs.copyToLocalFile(new Path(src_path+filename),new Path(filename_new));
        }
        else{
            fs.copyToLocalFile(new Path(src_path+filename),new Path(filename));
        }
//        FileSystem fs = FileSystem.get(new URI(hdfs),conf);
//        fs.copyToLocalFile(new Path(src_path+filename),new Path(filename));
        fs.close();
        System.out.println("download done");
    }


}

