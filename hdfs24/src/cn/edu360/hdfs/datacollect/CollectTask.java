package cn.edu360.hdfs.datacollect;

import java.io.File;
import java.io.FilenameFilter;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.TimerTask;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

public class CollectTask extends TimerTask {

	@Override
	public void run() {
		
		/*
		 * 	������ʱ̽����־ԴĿ¼
			������ȡ��Ҫ�ɼ����ļ�
			�����ƶ���Щ�ļ���һ�����ϴ���ʱĿ¼
			�����������ϴ�Ŀ¼�и��ļ�����һ���䵽HDFS��Ŀ��·����ͬʱ��������ɵ��ļ��ƶ�������Ŀ¼
		 */ 
		
		//����־������һ��log4j��־����
		Logger logger = Logger.getLogger("logRollingFile");
		//SimpleDateFormat�е�parse��������  
		//��String�͵��ַ���ת�����ض���ʽ��date����
		//��ȡ���βɼ�ʱ������
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH");
        String day = sdf.format(new Date());
		//����ԴĿ¼
		File srcdir = new File("e:/logs/accesslog/");
		//�г���־ԴĿ¼����Ҫ�ɼ����ļ�
		File[] listFiles = srcdir.listFiles(new FilenameFilter() {
			//�ļ�������������Ҫ�ɼ����ļ�������true���򷵻�false
			@Override
			public boolean accept(File dir, String name) {
				//������access��log��ͷ���ļ���
				if (name.startsWith("access.log.")){
					return true;
				}
				return false;
			}
		});
		
		//��¼��־
		logger.info("̽�⵽�����ļ���Ҫ�ɼ�"+Arrays.toString(listFiles));
		
	try {
		//��Ҫ�ɼ����ļ��ƶ������ϴ���ʱĿ¼
		File toUploaddir = new File("e:/logs/toupload/");
		for(File file:listFiles){
			//�˴������������һ���ͱ����ı���������ļ��������ļ���д�������ļ�toupload��
			FileUtils.moveFileToDirectory(file, toUploaddir, true);//���ļ������ļ���������ļ��в����ھʹ���
//			file.renameTo(toUploaddir);
			
		}
		
		//��¼��־
		logger.info("�����ļ��ƶ����˴��ϴ��ļ�"+toUploaddir.getAbsolutePath() );
		
		
			//����һ��hdfs�Ŀͻ��˶���
			FileSystem fs = FileSystem.get(new URI("hdfs://zhu:9000"), new Configuration(), "root");
			File[] toUploaddirs = toUploaddir.listFiles();
			//���HDFS�е�����Ŀ¼�Ƿ���ڣ�����������򴴽�
			Path hdfsDesPath = new Path("/logs/"+day);
			if (!fs.exists(hdfsDesPath)){
				fs.mkdirs(hdfsDesPath);
			}
			//��鱾��Ŀ¼�ı����ļ��Ƿ��������������򴴽�
			File backupDir = new File("e:/logs/backup/"+day+"/");
			if (!backupDir.exists()){
				backupDir.mkdirs();
			}
			
			for(File file:toUploaddirs){
				//�����ļ���HDFS������
				Path destPath = new Path(hdfsDesPath+"/"+"/access_log_"+UUID.randomUUID()+".log");
				fs.copyFromLocalFile(new Path(file.getAbsolutePath()), destPath);
				
				//��¼��־
				logger.info("�ļ�����hdfs���"+file.getAbsolutePath()+"-->"+destPath);
				
//				file.renameTo(backupDir);
				FileUtils.moveFileToDirectory(file, backupDir, true);
				
				//��¼��־
				logger.info("�ļ��������"+file.getAbsolutePath()+"-->"+backupDir);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

}
