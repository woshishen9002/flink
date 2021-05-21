package org.apache.flink.table.examples.java.util;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Test {

	public static void main(String[] args) {
		try {
			String dbURL = "jdbc:clickhouse://[10.160.162.33:8123#10.160.162.34:8123#10.160.162.51:8123]/test_local";
			//jdbc:clickhouse://10.160.162.33:8123#10.160.162.34:8123#10.160.162.51:8123#10.160.162.52:8123/test_local

			/*String prefix = dbURL.substring(0, dbURL.indexOf("["));
			String suffix = dbURL.substring(dbURL.indexOf("]")+1,dbURL.length());
			String hosts = dbURL.substring(dbURL.indexOf("[")+1, dbURL.indexOf("]"));

			System.out.println(prefix);
			System.out.println(suffix);
			System.out.println(hosts);*/

			initDbURLMap(dbURL);

			String s0 = takeDbURLFromMap(0, 2);
			String s1 = takeDbURLFromMap(1, 2);
			/*String s2 = takeDbURLFromMap(2, 6);
			String s3 = takeDbURLFromMap(3, 6);
			String s4 = takeDbURLFromMap(4, 6);
			String s5 = takeDbURLFromMap(5, 6);
			String s6 = takeDbURLFromMap(6, 6);*/
			System.out.println(s0+"--"+s1);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static Map<String,Long> dbURLMap;
	private static List<String> dbURLArray;

	private static void initDbURLMap(String dbURL){
		dbURLMap = new ConcurrentHashMap<>();
		dbURLArray = new ArrayList<>();
		if(dbURL.contains("[") && dbURL.contains("]")){
			String prefix = dbURL.substring(0, dbURL.indexOf("["));
			String suffix = dbURL.substring(dbURL.indexOf("]")+1,dbURL.length());
			String hosts = dbURL.substring(dbURL.indexOf("[")+1, dbURL.indexOf("]"));

			if(hosts != null){
				String[] hostArray = hosts.split("#");
				for (String host : hostArray) {
					//拼接为可用的url
					String dbUrl = prefix+host+suffix;

					dbURLMap.put(dbUrl, 0L);
					dbURLArray.add(dbUrl);
				}
			}
		}else{
			dbURLMap.put(dbURL, 0L);
			dbURLArray.add(dbURL);
		}
	}

	private static String takeDbURLFromMap(int taskNumber, int numTasks) {
		if(dbURLMap != null){
			int urlSize = dbURLArray.size();

			int index = 0;
			if(urlSize <= numTasks){
				index = taskNumber%urlSize;
			}else{ //url个数多于 task , 走roudrobin模式
				index = taskNumber;
			}

			String dbUrl = dbURLArray.get(index);
			if(dbUrlIsUseful(dbUrl)){
				return dbUrl;
			}else{
				taskNumber++;
				String s = takeDbURLFromMap(taskNumber, numTasks);
				if(s != null){
					return s;
				}
			}
		}
		return null;
	}

	private static boolean dbUrlIsUseful(String dbUrl){
		if(dbUrl != null){
			Long timestamp = dbURLMap.get(dbUrl);
			//不可用时间没超过5分钟，跳过该url
			if(timestamp == 0L || (System.currentTimeMillis() - timestamp) > 5*60*1000){
				return true;
			}
		}
		return false;
	}

}
