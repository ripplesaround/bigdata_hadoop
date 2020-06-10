package Final.DataClean;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import DataClean.DataClean;
import Final.DataClean.GoodsList;
import Final.DataClean.UserGoodsMatrix;

public class Entry {
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();  
        if (args.length != 2) {
//            System.err.println("参数错误");
//            System.exit(2);
			args = new String[2];
			args[0] = "input/file0";
			args[1] = "recinput/test4";
        }
      	FileSystem fs = FileSystem.get(conf);  

      	String[] args1 = {args[0],"/output"};
      	DataClean.main(args1);
      	
      	Path localPath = new Path("/output/part-r-00000");
      	Path remotePath = new Path("/test/test2");
      	if(fs.exists(remotePath)) {            
  			fs.delete(remotePath, true);       
      	}
      	fs.rename(localPath,remotePath);
      	
      	GoodsList.main(args1);
      	
      	String[] args2 = {"/test/test2","/test/test1","/output"};
      	UserGoodsMatrix.main(args2);
      	localPath = new Path("/output/part-r-00000");
      	remotePath = new Path("/test/test3");
      	if(fs.exists(remotePath)) {            
  			fs.delete(remotePath, true);       
      	}
      	fs.rename(localPath,remotePath);
      
      	FSDataInputStream fin = fs.open(new Path("/test/test1"));
      	BufferedReader  in = new BufferedReader(new InputStreamReader(fin,"UTF-8"));
      	String v1 = in.readLine();
      	v1="user "+"商品ID("+v1+") "+"age "+"gender"+"\r\n";
      	
      	fin = fs.open(new Path("/test/test3"));
      	in = new BufferedReader(new InputStreamReader(fin,"UTF-8"));
      	String lineTxt = null;
      	while((lineTxt = in.readLine()) != null){ 
            v1=v1+lineTxt+"\r\n";
        } 
      	fin.close();

      	FSDataOutputStream fout = fs.create(new Path(args[1]));
      	BufferedWriter out = new BufferedWriter(new OutputStreamWriter(fout, "UTF-8"));
      	out.write(v1);
      	out.close();

      	return ;
	}
}
