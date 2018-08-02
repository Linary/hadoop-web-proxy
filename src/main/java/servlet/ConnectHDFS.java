package servlet;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ConnectHDFS {

	public static void main(String[] args) throws IOException {
		// hdfs的路径
		String rootPath = "hdfs://hkg02-ns";

		Configuration conf = new Configuration();
		FileSystem coreSys = FileSystem.get(URI.create(rootPath), conf);
		Path targetDir = new Path(rootPath + "/user/work/");
		
		// 打印该目录下的所有文件的基本信息
		FileStatus[] fileStatus = coreSys.listStatus(targetDir);
		for (FileStatus file : fileStatus) {
			System.out.println(file.getPath() + "--" + file.getGroup() + "--" + file.getBlockSize() + "--"
					+ file.getLen() + "--" + file.getModificationTime() + "--" + file.getOwner());
		}
		
	}

}
