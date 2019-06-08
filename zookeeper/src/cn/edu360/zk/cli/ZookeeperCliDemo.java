package cn.edu360.zk.cli;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;

import javax.sound.midi.VoiceStatus;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Before;
import org.junit.Test;

public class ZookeeperCliDemo {
	ZooKeeper zk=null;
	@Before
	public void init() throws Exception{
		// ����һ������zookeeper�Ŀͻ��˶���
		zk=new ZooKeeper("zhu:2181,spark1:2181,spark2:2181", 2000, null);
	}
	//����
	@Test
	public void testCreate() throws Exception{
		// ����1��Ҫ�����Ľڵ�·��  ����2������  ����3������Ȩ��  ����4���ڵ�����       
		//�ڶ�������data����̫����Ϊ�����ȸ�leaderȻ��ָ�follow��
		//���̫��ͬ����ʱ���̫�����ݾͲ�һ�£��ֲ�ʽ�ͳ������ˣ�ϵͳ2�������1M
		String create = zk.create("/zhu", "hello eclipses".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		System.out.println(create);
		zk.close();
	}
	//��������
	@Test
	public void testUpdate() throws Exception{
		// ����1���ڵ�·��   ����2������    ����3����Ҫ�޸ĵİ汾��-1�����κΰ汾
		zk.setData("/zhu", "�Һ�˧".getBytes("UTF-8"), -1);
		
		zk.close();
	}
	//�õ�����
	@Test
	public void testGet() throws Exception{
		// ����1���ڵ�·��    ����2���Ƿ�Ҫ����    ����3����Ҫ��ȡ�����ݵİ汾,null��ʾ���°汾
		byte[] data = zk.getData("/zhu",false, null);
		System.out.println(new String(data,"UTF-8"));
		
		zk.close();
	}
	
	//�о�����
	@Test
	public void tesListChild() throws Exception{
		// ����1���ڵ�·��    ����2���Ƿ�Ҫ����   
		// ע�⣺���صĽ����ֻ���ӽڵ����֣�����ȫ·��
		List<String> children = zk.getChildren("/zookeeper", false);
		
		for (String child : children) {
			System.out.println(child);
		}
		
		zk.close();
	}
	//ɾ��
	@Test
	public void testRm() throws InterruptedException, KeeperException{
		
		zk.delete("/zhu", -1);
		
		zk.close();
	}
	
	

}
