package listen;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;


public class MyListener implements ServletContextListener{
//
//	 public MyListener() {
//		super();
//	}
	@Override
	public void contextDestroyed(ServletContextEvent arg0) {
		System.out.println("�����������ˡ�����");
		
	}

	@Override
	public void contextInitialized(ServletContextEvent arg0) {
//		//���Ǳ������Ķ���---servletContext
//		ServletContext servletContext = arg0.getServletContext();
//		//getsource���Ǳ������Ķ���  ��ͨ�õķ���
//		Object source = arg0.getSource();
//		System.out.println("�����������ˡ�����");
		//����һ����Ϣ����ĵ���----ÿ������12���Ϣһ��
		Timer timer = new Timer();
		//task:���� firstTime:��һ��ִ��ʱ��period :���ִ��ʱ��
//		timer.scheduleAtFixedRate(task, delay, period);
//		timer.scheduleAtFixedRate(new TimerTask() {
//			
//			@Override
//			public void run() {
//				System.out.println("��Ϣ�ˡ���");
//				
//			}
//		}, new Date(), 5000);
		//�޸ĳ�������ʵ��Ϣҵ��
		//1.��ʼʱ�䣺���������12��
		//2.���ʱ�䣺24Сʱ
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		String currentTime = "2018-7-8 00:29:30";
		Date parse=null;
		try {
			parse =format.parse(currentTime);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		timer.scheduleAtFixedRate(new TimerTask() {
			
			@Override
			public void run() {
				System.out.println("��Ϣ�ˡ���");
				
			}
		}, parse, 24*60*60*1000);
	}

}
