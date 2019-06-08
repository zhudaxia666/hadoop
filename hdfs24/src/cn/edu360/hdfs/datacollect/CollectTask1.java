package cn.edu360.hdfs.datacollect;

import java.io.File;
import java.io.FilenameFilter;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;
import java.util.TimerTask;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

public class CollectTask1 extends TimerTask {
	//将一些常量改到配置文件collect.properties中

	@Override
	public void run() {
		
		/*
		 * 	——定时探测日志源目录
			——获取需要采集的文件
			——移动这些文件到一个待上传临时目录
			——遍历待上传目录中各文件，逐一传输到HDFS的目标路径，同时将传输完成的文件移动到备份目录
		 */ 
	try {
		// 获取配置参数
		Properties props = PropertyHolderLazy.getProps();
		//打日志。构造一个log4j日志对象
		Logger logger = Logger.getLogger("logRollingFile");
		//SimpleDateFormat中的parse方法可以  
		//把String型的字符串转换成特定格式的date类型
		//获取本次采集时的日期
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH");
        String day = sdf.format(new Date());
		//创建源目录
		File srcdir = new File(props.getProperty(Constants.LOG_SOURCE_DIR));
		//列出日志源目录中需要采集的文件
		File[] listFiles = srcdir.listFiles(new FilenameFilter() {
			//文件名过滤器，需要采集的文件名返回true否则返回false
			@Override
			public boolean accept(File dir, String name) {
				//名字以access。log开头的文件名
				if (name.startsWith(props.getProperty(Constants.LOG_LEGAL_PREFIX))){
					return true;
				}
				return false;
			}
		});
		
		//记录日志
		logger.info("探测到如下文件需要采集"+Arrays.toString(listFiles));
		
	
		//将要采集的文件移动到待上传临时目录
		File toUploaddir = new File(props.getProperty(Constants.LOG_TOUPLOAD_DIR));
		for(File file:listFiles){
			//此处如果不进行这一步就报错，文本会把上述文件夹名当文件名写入上述文件toupload内
			FileUtils.moveFileToDirectory(file, toUploaddir, true);//将文件传到文件夹内如果文件夹不存在就创建
//			file.renameTo(toUploaddir);
			
		}
		
		//记录日志
		logger.info("上述文件移动到了待上传文件"+toUploaddir.getAbsolutePath() );
		
		
			//构造一个hdfs的客户端对象
			FileSystem fs = FileSystem.get(new URI(props.getProperty(Constants.HDFS_URI)), new Configuration(), "root");
			File[] toUploaddirs = toUploaddir.listFiles();
			//检查HDFS中的日期目录是否存在，如果不存在则创建
			Path hdfsDesPath = new Path(props.getProperty(Constants.HDFS_DEST_BASE_DIR)+day);
			if (!fs.exists(hdfsDesPath)){
				fs.mkdirs(hdfsDesPath);
			}
			//检查本地目录的备份文件是否存在如果不存在则创建
			File backupDir = new File(props.getProperty(Constants.LOG_BACKUP_BASE_DIR)+day+"/");
			if (!backupDir.exists()){
				backupDir.mkdirs();
			}
			
			for(File file:toUploaddirs){
				//传输文件到HDFS并改名
				Path destPath = new Path(hdfsDesPath+"/"+UUID.randomUUID()+props.getProperty(Constants.HDFS_FILE_SUFFIX));
				fs.copyFromLocalFile(new Path(file.getAbsolutePath()), destPath);
				
				//记录日志
				logger.info("文件传输hdfs完成"+file.getAbsolutePath()+"-->"+destPath);
				
//				file.renameTo(backupDir);
				FileUtils.moveFileToDirectory(file, backupDir, true);
				
				//记录日志
				logger.info("文件备份完成"+file.getAbsolutePath()+"-->"+backupDir);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

}
