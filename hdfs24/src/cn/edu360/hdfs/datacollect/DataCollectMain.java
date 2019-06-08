package cn.edu360.hdfs.datacollect;

import java.util.Timer;

public class DataCollectMain {

	public static void main(String[] args) {
		Timer timer = new Timer();
		//schedule是等前一个任务执行完才执行下一个，erscheduleAt..为每个一段时间就启动，不管前面的任务完没完成
		//每隔一个小时采集一次
		//数据采集的任务
//		timer.schedule(new CollectTask(), 0, 60*60*1000L);
		timer.schedule(new CollectTask1(), 0, 60*60*1000L);//使用常量的配置文件
		//数据清理的任务
//		timer.schedule(new BackupCleanTask(), 0, 60*60*1000L);
		timer.schedule(new BackupCleanTask1(), 0, 60*60*1000L);

	}

}
