package cn.edu360.hdfs.datacollect;

import java.util.Timer;

public class DataCollectMain {

	public static void main(String[] args) {
		Timer timer = new Timer();
		//schedule�ǵ�ǰһ������ִ�����ִ����һ����erscheduleAt..Ϊÿ��һ��ʱ�������������ǰ���������û���
		//ÿ��һ��Сʱ�ɼ�һ��
		//���ݲɼ�������
//		timer.schedule(new CollectTask(), 0, 60*60*1000L);
		timer.schedule(new CollectTask1(), 0, 60*60*1000L);//ʹ�ó����������ļ�
		//�������������
//		timer.schedule(new BackupCleanTask(), 0, 60*60*1000L);
		timer.schedule(new BackupCleanTask1(), 0, 60*60*1000L);

	}

}
