package cn.edu360.zk.disributesystem;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.jboss.netty.handler.queue.BufferedWriteHandler;
/*
 * 客户端 需要打包
 */
public class Consumer {
	//定义一个list用于存放最新在线服务器列表
	private volatile ArrayList<String> onlineServers = new ArrayList<>();//加volatile为让onlineServers同步。
	//因为getOnlineServer()这个线程更新onlineServers=servers时在sendRequest不一定立马看的到，会有一点延迟，异常多出一两次
	ZooKeeper zk=null;
	//构造zk客户端连接
	public void connectZK()throws Exception{
		
		zk=new ZooKeeper("zhu:2181,spark1:2181,spark2:2181", 2000,new Watcher(){

			@Override
			public void process(WatchedEvent event) {
				if (event.getState()==KeeperState.SyncConnected && event.getType()==EventType.NodeChildrenChanged){
					try {
						// 事件回调逻辑中，再次查询zk上的在线服务器节点即可，查询逻辑中又再次注册了子节点变化事件监听
						getOnlineServer();
						
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
				
			}
			
		});
		
	}
	
	public void sendRequest() throws Exception{
		Random random = new Random();
		while(true){
			try{
			//挑选一台在线的服务器
			int nextInt = random.nextInt(onlineServers.size());//随机挑选一台
			String server = onlineServers.get(nextInt);
			String hostname = server.split(":")[0];
			int port = Integer.parseInt(server.split(":")[1]);
			
			System.out.println("本次请求挑选的服务器为：" + server);
			
			Socket socket = new Socket(hostname,port);
			OutputStream out = socket.getOutputStream();
			InputStream in = socket.getInputStream();
			out.write("haha".getBytes());
			out.flush();//
			
			byte[] buf = new byte[256];
			int read = in.read(buf);
			
			System.out.println("服务器响应的时间为：" + new String(buf, 0, read));

			out.close();
			in.close();
			socket.close();

			Thread.sleep(2000);
			}catch (Exception e) {
				e.printStackTrace();//这个try-catch是为了防止关掉一个服务器。如果这是客户端正好访问那台关掉的服务器，就会报错得情况
			}
			
			
		}
	}
	
	//查询在线服务器列表
	public void getOnlineServer() throws Exception{
		List<String> children = zk.getChildren("/servers", true);
		ArrayList<String> servers = new ArrayList<>();
		for (String child : children) {
			
			byte[] data = zk.getData("/servers/"+child, false, null);
			String serverInfo = new String(data);
			servers.add(serverInfo);
		}
		
		onlineServers=servers;
		System.out.println("查询了一次zk，当前在线的服务器有：" + servers);
		
	}
	
	public static void main(String[] args) throws Exception {
		
		Consumer consumer = new Consumer();
		//构造zk客户端连接
		consumer.connectZK();
		
		//查询在线服务器列表
		consumer.getOnlineServer();
		
		//处理业务（向一台服务器发送时间查询请求）
		consumer.sendRequest();
	}

}
