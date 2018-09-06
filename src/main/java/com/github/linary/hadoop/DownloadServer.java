/*
 * Copyright (C) 2018 Baidu, Inc. All Rights Reserved.
 */
package com.github.linary.hadoop;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

/**
 * 1）初始化工作，做一些安全性检查 2）解析客户端发送的请求Range头 3）发送Range对应部分的字节数据
 * 
 * @author liningrui
 */

// 访问URL：http://localhost:8080/hadoop-web-proxy/p1/work/20151104023705_10.58.188.53.log

public class DownloadServer extends HttpServlet {

	// 常量定义
	private static final long serialVersionUID = 1L;

	private static Logger logger = Logger.getLogger(DownloadServer.class);

	// 默认缓冲区大小 20KB.
	private static final int DEFAULT_BUFFER_SIZE = 20480;

	private int BUFFER_SIZE;
	private String HDFS_PATH;
	private String BASE_PATH;

	private Configuration conf;

	/**
	 * 准备一些文件信息相关的临时变量，后面需要用到，包括： 文件路径、文件名、文件大小、上一次修改时间、过期时间、Etag
	 */
	protected class Aux {
		public Path filePath;
		public FileSystem fileSystem;
		public long totalLength;
		public long lastModified;
		public String eTag;
		public List<Range> ranges;
	}

	@Override
	public void init() throws ServletException {
		// HDFS的路径
		HDFS_PATH = this.getInitParameter("HDFS_PATH");
		if (HDFS_PATH.isEmpty()) {
			// 这里应该要记录日志比较好
			logger.error("Need to specify the hdfs url and root directory for the download file.");
			// 抛出一个运行时异常
			throw new RuntimeException("Need to specify the hdfs url and root directory for the download file.");
		}

		// 获取HDFS版本号
		BASE_PATH = this.getInitParameter("BASE_PATH");
		if (BASE_PATH.isEmpty()) {
			logger.error("Need to specify the base path in file 'web.xml'.");
			throw new RuntimeException("Need to specify the base path in file 'web.xml'.");
		}

		// 根据本地配置文件初始化配置对象
		conf = new Configuration();
		conf.addResource("proxy-site.xml");

		String BUF_SIZE_STR = conf.get("BUFFER_SIZE");
		if (BUF_SIZE_STR == null) {
			BUFFER_SIZE = DEFAULT_BUFFER_SIZE;
		} else {
			BUFFER_SIZE = Integer.parseInt(BUF_SIZE_STR);
			if (BUFFER_SIZE <= 0) {
				logger.error("The param 'BUFFER_SIZE' must be a positive integer number.");
				throw new RuntimeException("The param 'BUFFER_SIZE' must be a positive integer number.");
			}
		}
	}

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse res) throws ServletException, IOException {
		String clientIP = getClientIP(req);
		try {
			// 初始化所需参数，并做一些基本的安全检查
			Aux aux = initializeAuxObject(req, res);
			if (aux == null) {
				return;
			}
			// 记录日志
			logger.info("client " + clientIP + " try to download hdfs file " + aux.filePath.toString());
			// 检查请求头中的范围是否符合规范
			if (validateRequestRange(req, res, aux)) {
				// 发送需要的数据
				sendRequestedFile(req, res, aux);
			}
			// 记录日志
			logger.info("client " + clientIP + " download hdfs file " + aux.filePath.toString() + " succeed.");
		} catch (Exception e) {
			logger.error(e);
		}
	}

	/**
	 * 初始化后续方法所需的参数，并做一些安全性检查
	 * 
	 * @param req
	 *            客户端发送给服务器的HTTP请求
	 * @param res
	 *            服务器发送给客户端的HTTP响应
	 * @throws IOException
	 *             访问资源的地址错误
	 */
	private Aux initializeAuxObject(HttpServletRequest req, HttpServletResponse res) throws IOException {
		Aux aux = new Aux();

		// 从用户请求的URI地址中寻找待下载文件路径标记的索引
		String requestURI = req.getRequestURI();
		// 寻找到第一个 BASE_PATH 出现的位置
		int baseIndex = requestURI.indexOf(BASE_PATH) + BASE_PATH.length() + 1;

		// 后面多次需要用到的 Path 对象
		aux.filePath = new Path(HDFS_PATH + requestURI.substring(baseIndex));
		// hdfs 的文件系统，这里可以不是同一个HDFS
		aux.fileSystem = FileSystem.get(URI.create(aux.filePath.toString()), conf);
		// 检查该路径的文件是否存在、且指定路径是文件而不是目录
		if (!aux.fileSystem.exists(aux.filePath) || !aux.fileSystem.isFile(aux.filePath)) {
			res.sendError(HttpServletResponse.SC_NOT_FOUND);
			logger.error(aux.filePath.toString() + " is not a valid hdfs file path.");
			return null;
		}

		// 判断如果当前路径资源是单个文件还是一个目录
		if (aux.fileSystem.isFile(aux.filePath)) {
			
		} else {
			
		}
		
		
		// 得到文件状态信息
		FileStatus fileStatus = aux.fileSystem.getFileStatus(aux.filePath);
		// 后续几个方法需要用到的成员变量
		aux.totalLength = fileStatus.getLen();
		aux.lastModified = fileStatus.getModificationTime();
		// etag并没有固定的算法要求，可以自己定义生成规则
		aux.eTag = Long.toHexString(aux.lastModified) + "-" + Long.toHexString(aux.totalLength);
		return aux;
	}

	/**
	 * 检查并解析请求头中的Range部分，判断客户端请求字节范围是否合理
	 * 
	 * @param req
	 *            客户端发送给服务器的HTTP请求
	 * @param res
	 *            服务器发送给客户端的HTTP响应
	 * @throws IOException
	 * @return Range参数不存在或者Range参数格式符合规范都返回true，否则返回false
	 */
	private boolean validateRequestRange(HttpServletRequest req, HttpServletResponse res, Aux aux) throws IOException {
		// 保存请求的多段 range
		aux.ranges = new ArrayList<Range>();

		String range = req.getHeader("Range");
		// 如果请求中没有发送range参数，表明需要下载整个文件
		if (range == null) {
			// 表征整个文件范围的变量
			aux.ranges.add(new Range(0, aux.totalLength - 1));
			return true;
		}

		// Range头的形式为："bytes=n-n,n-n,n-n..."，如果不是，返回416状态码（请求范围无法满足）
		if (!range.matches("^bytes=\\d*-\\d*(,\\d*-\\d*)*$")) {
			// 返回文件的大小
			res.setHeader("Content-Range", "bytes */" + aux.totalLength);
			res.sendError(HttpServletResponse.SC_REQUESTED_RANGE_NOT_SATISFIABLE);
			logger.error("the format of the requested range is not matched with ^bytes=\\d*-\\d*(,\\d*-\\d*)*$.");
			return false;
		}

		for (String part : range.substring("bytes=".length()).split(",")) {
			// 提取开始和结束字节位置，左闭右开
			long start = subLong(part, 0, part.indexOf("-"));
			long end = subLong(part, part.indexOf("-") + 1, part.length());

			if (start == -1) {
				start = 0;
			} else if (end == -1 || end > aux.totalLength - 1) {
				end = aux.totalLength - 1;
			}
			// 检查range是否有效，否则返回416
			if (start > end) {
				res.setHeader("Content-Range", "bytes */" + aux.totalLength);
				res.sendError(HttpServletResponse.SC_REQUESTED_RANGE_NOT_SATISFIABLE);
				logger.error("the start of the requested range is bigger than end.");
				return false;
			}
			// 添加当前 range
			aux.ranges.add(new Range(start, end));
		}
		return true;
	}

	/**
	 * 向客户端发送器请求的文件
	 * 
	 * @param req
	 *            客户端发送给服务器的HTTP请求
	 * @param res
	 *            服务器发送给客户端的HTTP响应
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	private void sendRequestedFile(HttpServletRequest req, HttpServletResponse res, Aux aux)
			throws IllegalArgumentException, IOException {
		// 为什么需要 reset()
		res.reset();
		res.setBufferSize(BUFFER_SIZE);
		res.setHeader("content-disposition", "attachment; filename=\"" + aux.filePath.getName() + "\"");
		res.setDateHeader("Last-Modified", aux.lastModified);
		res.setHeader("ETag", aux.eTag);
		// response 压缩设置
		// res.setHeader("Content-Encoding", "gzip");

		// hdfs 文件的输入流，用于读取hdfs文件
		FSDataInputStream input = aux.fileSystem.open(aux.filePath);
		// HTTP 响应的输出流，用于写入客户端文件
		OutputStream output = res.getOutputStream();
		try {
			if (aux.ranges.size() == 1) {
				Range r = aux.ranges.get(0);
				// 全范围
				Range fullRange = new Range(0, aux.totalLength - 1);
				// 如果是全范围，返回一个200的状态码，否则返回206
				if (r.equals(fullRange)) {
					// 请求的是全范围时不需要返回"Content-Range"
					res.setStatus(HttpServletResponse.SC_OK);
					res.setHeader("Accept-Ranges", "bytes");
				} else {
					res.setStatus(HttpServletResponse.SC_PARTIAL_CONTENT);
					res.setHeader("Content-Range", "bytes " + r.start + "-" + r.end + "/" + aux.totalLength);
				}
				res.setContentType("application/octet-stream");
				res.setHeader("Content-Length", String.valueOf(r.length));

				// 拷贝一个range
				copy(input, output, r.start, r.length);
			} else {
				res.setStatus(HttpServletResponse.SC_PARTIAL_CONTENT);
				// 多段数据间的分割符，用 UUID 生成
				String multipartBoundary = UUID.randomUUID().toString();
				res.setContentType("multipart/byteranges; boundary=" + multipartBoundary);
				// 获取当前系统的换行符，默认是"\n"
				String lineSeparator = System.getProperty("line.separator", "\n");
				for (Range r : aux.ranges) {
					// 发送多段数据时，需要在body进行组织
					output.write(lineSeparator.getBytes());
					output.write(("--" + multipartBoundary + lineSeparator).getBytes());
					output.write(("Content-Type: " + "application/octet-stream" + lineSeparator).getBytes());
					output.write(
							("Content-Range: bytes " + r.start + "-" + r.end + "/" + aux.totalLength + lineSeparator)
									.getBytes());
					output.write(lineSeparator.getBytes());
					// 这里不用做一些额外的处理吗？
					copy(input, output, r.start, r.length);
				}
				// 最后要加一个边界
				output.write((lineSeparator + "--" + multipartBoundary + "--" + lineSeparator).getBytes());
			}
		} finally {
			close(output);
			close(input);
		}
	}

	/**
	 * 关闭打开的资源
	 * 
	 * @param resource
	 */
	private void close(Closeable resource) {
		if (resource != null) {
			try {
				resource.close();
			} catch (IOException e) {
				logger.error(e);
			}
		}
	}

	/**
	 * 把HDFS输入流中的数据拷贝到响应的输出流中
	 * 
	 * @param input
	 * @param output
	 * @param start
	 * @param length
	 * @throws IOException
	 */
	private void copy(FSDataInputStream input, OutputStream output, long start, long length) throws IOException {
		byte[] buffer = new byte[BUFFER_SIZE];
		int read;

		// 就移位到指定位置
		input.seek(start);
		long toRead = length;

		while ((read = input.read(buffer)) != -1) {
			// 注意：每次比较toRead的值就变更了
			if ((toRead -= read) > 0) {
				output.write(buffer, 0, read);
			} else {
				output.write(buffer, 0, (int) toRead + read);
				break;
			}
		}
	}

	/**
	 * 提取指定范围内的子字符串的长整型值
	 * 
	 * @param str
	 *            源字符串
	 * @param beginIndex
	 *            范围起始索引
	 * @param endIndex
	 *            范围终止索引
	 * @return 如果指定范围没有数据，返回-1
	 */
	private long subLong(String str, int beginIndex, int endIndex) {
		String substring = str.substring(beginIndex, endIndex);
		return (substring.length() > 0) ? Long.parseLong(substring) : -1;
	}

	/**
	 * 辅助类，便于进行字节范围的相关操作
	 */
	protected class Range {
		long start;
		long end;
		long length;

		public Range(long start, long end) {
			this.start = start;
			this.end = end;
			this.length = end - start + 1;
		}

		@Override
		public boolean equals(Object obj) {
			Range other = (Range) obj;
			return start == other.start && end == other.end && length == other.length;
		}
	}

	
	/**
	 * 获取客户端的真实IP
	 * @param request
	 * @return
	 */
	public String getClientIP(HttpServletRequest request) {
		// 获取请求主机IP地址,如果通过代理进来，则透过防火墙获取真实IP地址  
        String ip = request.getHeader("X-Forwarded-For");  
        if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {  
            if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {  
                ip = request.getHeader("Proxy-Client-IP");  
            }  
            if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {  
                ip = request.getHeader("WL-Proxy-Client-IP");  
            }  
            if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {  
                ip = request.getHeader("HTTP_CLIENT_IP");  
            }  
            if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {  
                ip = request.getHeader("HTTP_X_FORWARDED_FOR");  
            }  
            if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {  
                ip = request.getRemoteAddr();  
            }  
        } else if (ip.length() > 15) {  
            String[] ips = ip.split(",");  
            for (int index = 0; index < ips.length; index++) {  
                String strIp = (String) ips[index];  
                if (!("unknown".equalsIgnoreCase(strIp))) {  
                    ip = strIp;  
                    break;  
                }  
            }  
        }  
        return ip;  
	}

}
