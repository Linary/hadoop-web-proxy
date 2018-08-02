package servlet;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ReadFromHDFS {

	public static void main(String[] args) throws IOException {
		readFromHdfs();
	}
	
	
	public static void readFromHdfs() throws FileNotFoundException, IOException {
		// hdfs文件路径
		String hdfs_address = "hdfs://hkg02-ns/user/work/20151104023705_10.58.188.53.log";
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(hdfs_address), conf);
		FSDataInputStream hdfsInStream = fs.open(new Path(hdfs_address));

		OutputStream out = new FileOutputStream("d:/hdfs.txt");
		byte[] ioBuffer = new byte[1024];
		int readLen = hdfsInStream.read(ioBuffer);

		while (-1 != readLen) {
			out.write(ioBuffer, 0, readLen);
			readLen = hdfsInStream.read(ioBuffer);
		}
		out.close();
		hdfsInStream.close();
		fs.close();
	}
	
	
	/**
	 * 下载hdfs上的文件
	 * @param hdfs_address
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public static void readFromHdfs(String hdfs_address) throws FileNotFoundException, IOException {
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(hdfs_address), conf);
		FSDataInputStream hdfsInStream = fs.open(new Path(hdfs_address));

		OutputStream out = new FileOutputStream("d:/hdfs.txt");
		byte[] ioBuffer = new byte[1024];
		int readLen = hdfsInStream.read(ioBuffer);

		while (-1 != readLen) {
			out.write(ioBuffer, 0, readLen);
			readLen = hdfsInStream.read(ioBuffer);
		}
		out.close();
		hdfsInStream.close();
		fs.close();
	}
	
}
