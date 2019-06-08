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
		 * 1��������������
			     |-����������ͨ��ServerSocket
			     |-����������Socket���տͻ�������
			     |-����IO��������ȡ�ͻ��˷��͵�����
			     |-����IO�������ͻ��˷���������Ϣ
			2�������ͻ���
			     |-����Socketͨ�ţ�����ͨ�ŷ�������IP��Port
			     |-����IO����������������������Ϣ
			     |-����IO��������ȡ��������������������Ϣ
		 */
		try {
			ServerSocket ss = new ServerSocket(port);
			System.out.println("ҵ���߳��Ѱ󶨶˿�"+port+"׼���������Ѷ�������.....");
			while(true){
				Socket sc = ss.accept();//�õ�����
				InputStream inputStream = sc.getInputStream();//�õ��ͻ�������Ķ���
				OutputStream outputStream = sc.getOutputStream();//����Ҫʲô���ͷ��������Ǿ�
				outputStream.write(new Date().toString().getBytes());
			}
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	
}
