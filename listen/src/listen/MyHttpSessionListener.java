package listen;

import javax.servlet.http.HttpSessionEvent;
import javax.servlet.http.HttpSessionListener;

public class MyHttpSessionListener implements HttpSessionListener{

	@Override
	public void sessionCreated(HttpSessionEvent arg0) {
		System.out.println("session´´½¨"+arg0.getSession().getId());
		
	}

	@Override
	public void sessionDestroyed(HttpSessionEvent arg0) {
		System.out.println("sessionÏú»Ù");
		
	}

}
