package cn.edu360.zk.disributesystem;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Date;

public class TimeQueryService extends Thread{

	int port=0;
	public TimeQueryService(int port) {
		this.port=port;
	}
	
	@Override
	public void run() {
		/*
		 * 1、建立服务器端
			     |-服务器建立通信ServerSocket
			     |-服务器建立Socket接收客户端连接
			     |-建立IO输入流读取客户端发送的数据
			     |-建立IO输出流向客户端发送数据消息
			2、建立客户端
			     |-创建Socket通信，设置通信服务器的IP和Port
			     |-建立IO输出流向服务器发送数据消息
			     |-建立IO输入流读取服务器发送来的数据消息
		 */
		try {
			ServerSocket ss = new ServerSocket(port);
			System.out.println("业务线程已绑定端口"+port+"准备接受消费端请求了.....");
			while(true){
				Socket sc = ss.accept();//得到连接
				InputStream inputStream = sc.getInputStream();//拿到客户端请求的东西
				OutputStream outputStream = sc.getOutputStream();//管他要什么，就返回下面那句
				outputStream.write(new Date().toString().getBytes());
			}
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	
}
