package cn.edu360.hdfs.datacollect;

import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.TimerTask;

import org.apache.commons.io.FileUtils;
//import org.apache.hadoop.fs.FileUtil;

public class BackupCleanTask1 extends TimerTask {

	@Override
	public void run() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH");
		long now = new Date().getTime();
		
		try {
			Properties props = PropertyHolderLazy.getProps();
			//访问本地备份目录
			File backupdir = new File(props.getProperty(Constants.LOG_BACKUP_BASE_DIR));
			File[] daybacdir = backupdir.listFiles();
			
			//判断备份日期子目录是否超过24小时
			for(File dir:daybacdir){
				
					long time = sdf.parse(dir.getName()).getTime();
					if (now-time>24*60*60*1000L){
						FileUtils.deleteDirectory(dir);
					}
					
				}	
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			

	}

}
