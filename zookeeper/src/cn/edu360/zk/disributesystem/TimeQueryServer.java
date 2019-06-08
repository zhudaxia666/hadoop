package cn.edu360.zk.disributesystem;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
/*
 *  这是服务器端。打包步骤：export->java->run jar file->选择这个文件，保存e:\server.jar
 *  同理对客户端进行打包Consumer
 */
public class TimeQueryServer {
	ZooKeeper zk=null;
	//构造zk客户端连接
	public void connectZK()throws Exception{
		
		zk=new ZooKeeper("zhu:2181,spark1:2181,spark2:2181", 2000,null);
	}
	
	//注册服务器信息
	public void registerServerInfo(String hostname,String port) throws Exception{
		/**
		 * 先判断注册节点的父节点是否存在，如果不存在，则创建
		 */
		Stat stat = zk.exists("/servers", false);
		if (stat==null){
			zk.create("/servers", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
		
		//注册服务器数据到zk的约定注册节点下
		String create = zk.create("/servers/server", (hostname+":"+port).getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		
		System.out.println(hostname+" 服务器向zk注册信息成功，注册的节点为：" + create);
	}
	
	
	public static void main(String[] args) throws Exception {
		
		TimeQueryServer timeQueryServer = new TimeQueryServer();
		
		// 构造zk客户端连接
		timeQueryServer.connectZK();
		
		// 注册服务器信息
		timeQueryServer.registerServerInfo(args[0], args[1]);
		
		// 启动业务线程开始处理业务
		new TimeQueryService(Integer.parseInt(args[1])).start();
	}
	
	
	
	

}
