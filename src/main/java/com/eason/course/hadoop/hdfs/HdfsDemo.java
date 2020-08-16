package com.eason.course.hadoop.hdfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class HdfsDemo {

	public static final String FILE_PATH = "hdfs://vm-01:9000/d1/a.log";

	public static final String HDFS_PATH = "hdfs://vm-01:9000";

	public static void main(String[] args) throws Exception {
		Properties properties = System.getProperties();
		properties.setProperty("HADOOP_USER_NAME", "eason");
		// withInputStream();
		FileSystem fileSystem = FileSystem.get(new URI(HDFS_PATH + "/d2"), new Configuration());
//		//创建文件夹
//
//		fileSystem.mkdirs(new Path("/d2"));
		// 上传文件
//		FSDataOutputStream out = fileSystem.create(new Path(HDFS_PATH + "/d2/a.log"));
//		FileInputStream in = new FileInputStream(new File("D:\\Test.java"));
//		IOUtils.copyBytes(in, out, 1024 * 4, true);
		// 下载文件
//		FSDataInputStream in = fileSystem.open(new Path(HDFS_PATH + "/d2/a.log"));
//		IOUtils.copyBytes(in, System.out, 1024 * 4, true);
		// 删除文件

		boolean isSuccess = fileSystem.delete(new Path(HDFS_PATH + "/d2"), true);
		System.out.println(isSuccess);
	}

	private static void withInputStream() throws MalformedURLException, IOException {
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
		URL url = new URL(FILE_PATH);
		InputStream in = url.openStream();
		IOUtils.copyBytes(in, System.out, new Configuration(), true);
	}

}
