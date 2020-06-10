package SQL_Store;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

 
public class sqlpy{
	
		/**
		 * @param args
		 * @throws IOException 
		 * @throws InterruptedException 
		 */
		public static void main(String[] args) throws IOException,InterruptedException {
			String exe = "/Users/harry/anaconda3/bin/python";
			String command = "/Users/harry/Desktop/sql.py";
			String[] cmdArr = new String[] {exe,command};
			System.out.println(command);
			Process process = Runtime.getRuntime().exec(cmdArr);
		}
}