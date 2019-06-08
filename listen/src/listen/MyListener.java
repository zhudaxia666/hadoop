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
		System.out.println("监听器销毁了。。。");
		
	}

	@Override
	public void contextInitialized(ServletContextEvent arg0) {
//		//就是被监听的对象---servletContext
//		ServletContext servletContext = arg0.getServletContext();
//		//getsource就是被监听的对象  是通用的方法
//		Object source = arg0.getSource();
//		System.out.println("监听器创建了。。。");
		//开启一个计息任务的调度----每天晚上12点计息一次
		Timer timer = new Timer();
		//task:任务 firstTime:第一次执行时间period :间隔执行时间
//		timer.scheduleAtFixedRate(task, delay, period);
//		timer.scheduleAtFixedRate(new TimerTask() {
//			
//			@Override
//			public void run() {
//				System.out.println("计息了。。");
//				
//			}
//		}, new Date(), 5000);
		//修改成银行真实计息业务
		//1.起始时间：定义成晚上12点
		//2.间隔时间：24小时
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
				System.out.println("计息了。。");
				
			}
		}, parse, 24*60*60*1000);
	}

}
