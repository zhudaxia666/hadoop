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
		// 构造一个连接zookeeper的客户端对象
		zk=new ZooKeeper("zhu:2181,spark1:2181,spark2:2181", 2000, null);
	}
	//创建
	@Test
	public void testCreate() throws Exception{
		// 参数1：要创建的节点路径  参数2：数据  参数3：访问权限  参数4：节点类型       
		//第二个参数data不能太大，因为数据先给leader然后分给follow。
		//如果太大同步的时间就太长数据就不一致，分布式就出问题了，系统2最大允许1M
		String create = zk.create("/zhu", "hello eclipses".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		System.out.println(create);
		zk.close();
	}
	//更新内容
	@Test
	public void testUpdate() throws Exception{
		// 参数1：节点路径   参数2：数据    参数3：所要修改的版本，-1代表任何版本
		zk.setData("/zhu", "我好帅".getBytes("UTF-8"), -1);
		
		zk.close();
	}
	//得到数据
	@Test
	public void testGet() throws Exception{
		// 参数1：节点路径    参数2：是否要监听    参数3：所要获取的数据的版本,null表示最新版本
		byte[] data = zk.getData("/zhu",false, null);
		System.out.println(new String(data,"UTF-8"));
		
		zk.close();
	}
	
	//列举数据
	@Test
	public void tesListChild() throws Exception{
		// 参数1：节点路径    参数2：是否要监听   
		// 注意：返回的结果中只有子节点名字，不带全路径
		List<String> children = zk.getChildren("/zookeeper", false);
		
		for (String child : children) {
			System.out.println(child);
		}
		
		zk.close();
	}
	//删除
	@Test
	public void testRm() throws InterruptedException, KeeperException{
		
		zk.delete("/zhu", -1);
		
		zk.close();
	}
	
	

}
