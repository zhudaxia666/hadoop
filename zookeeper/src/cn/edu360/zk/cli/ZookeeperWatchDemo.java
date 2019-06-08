package cn.edu360.zk.cli;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;

import javax.sound.midi.VoiceStatus;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Before;
import org.junit.Test;

public class ZookeeperWatchDemo {
	ZooKeeper zk=null;
	@Before
	public void init() throws Exception{
		// 构造一个连接zookeeper的客户端对象
		zk=new ZooKeeper("zhu:2181,spark1:2181,spark2:2181", 2000, new Watcher(){

			@Override
			public void process(WatchedEvent event) {
				
				if (event.getState()==KeeperState.SyncConnected && event.getType()==EventType.NodeDataChanged){
					System.out.println(event.getPath());//收到的事件所发生的节点路径
					System.out.println(event.getType());//收到的事件类型
					System.out.println("赶紧换照片，换浴室里面的洗浴套装.....");//收到事件后，我们的处理逻辑
					try {
						
						zk.getData("/mygirls", true, null);//加这句可以反复监听，回调逻辑，继续注册监听。正常监听的节点变化只返回一次
						
					} catch (Exception e) {
						e.printStackTrace();
					}
				}else if (event.getState() == KeeperState.SyncConnected && event.getType() == EventType.NodeChildrenChanged) {
					
					System.out.println("子节点变化了。。。。。。");
					
				}
				
			}
			
		});
	}
	//创建
	@Test
	public void testWatch() throws Exception{
		
		byte[] data = zk.getData("/mygirls", true, null);//监听节点数据变化
		
		List<String> children = zk.getChildren("/mygirls", true);//监听节点的子节点变化事件
		
		System.out.println(new String(data,"UTF-8"));
		
		Thread.sleep(Long.MAX_VALUE);////睡眠。不然没法收到改变后的提示。
		//既然zk线程一直在运行。为什么还加这句。因为创建zookeeper对象时你就创建了守护线程，监听事件的线程是守护线程，等主线程执行完毕退出后。它也退了。例子见工程
	}
	
}
