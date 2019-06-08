package cn.edu360.hdfs.datacollect;

public class Constants {

	/**
	 * 日志源目录参数key
	 */
	public static final String LOG_SOURCE_DIR = "LOG_SOURCE_DIR";
	
	/**
	 * 日志待上传目录参数key
	 */
	//static 强调只有一份，final 说明是一个常量，final定义的基本类型的值是不可改变的，但是fianl定义的引用对象的值是可以改变的
	public static final String LOG_TOUPLOAD_DIR = "LOG_TOUPLOAD_DIR";
	
	
	public static final String LOG_BACKUP_BASE_DIR = "LOG_BACKUP_BASE_DIR";
	
	
	public static final String LOG_BACKUP_TIMEOUT = "LOG_BACKUP_TIMEOUT";
	
	
	public static final String LOG_LEGAL_PREFIX = "LOG_LEGAL_PREFIX";
	
	
	public static final String HDFS_URI = "HDFS_URI";
	
	
	public static final String HDFS_DEST_BASE_DIR = "HDFS_DEST_BASE_DIR";
	
	
	public static final String HDFS_FILE_PREFIX = "HDFS_FILE_PREFIX";
	
	
	public static final String HDFS_FILE_SUFFIX = "HDFS_FILE_SUFFIX";

}
