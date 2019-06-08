package cn.edu360.hdfs.datacollect;

import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimerTask;

import org.apache.commons.io.FileUtils;
//import org.apache.hadoop.fs.FileUtil;

public class BackupCleanTask extends TimerTask {

	@Override
	public void run() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH");
		long now = new Date().getTime();
		try {
			//���ʱ��ر���Ŀ¼
			File backupdir = new File("e:/logs/backup/");
			File[] daybacdir = backupdir.listFiles();
			
			//�жϱ���������Ŀ¼�Ƿ񳬹�24Сʱ
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
