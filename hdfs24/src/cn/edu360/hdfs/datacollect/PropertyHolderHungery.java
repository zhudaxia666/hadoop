package cn.edu360.hdfs.datacollect;

import java.util.Properties;

/*
 * 单例设计模式：方式1：饿汉式单例
 */

public class PropertyHolderHungery {
	private static Properties prop = new Properties();
	static{
		try{
			//load读你那个文件。启动程序有个class加载器，class加载器加载这个类时就自然而然知道这个类在哪，知道
			prop.load(PropertyHolderHungery.class.getClassLoader().getResourceAsStream("collect.properties"));
			
		}catch (Exception e) {
//			e.printStackTrace(s);
		}
	}
	public static Properties getProps() throws Exception{
		return prop;
	}

}
