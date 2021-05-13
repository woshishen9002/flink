/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc.internal.connection;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Simple JDBC connection provider.
 */
public class SimpleJdbcConnectionProvider implements JdbcConnectionProvider, Serializable {

	private static final Logger LOG = LoggerFactory.getLogger(SimpleJdbcConnectionProvider.class);

	private static final long serialVersionUID = 1L;

	private final JdbcConnectionOptions jdbcOptions;

	private transient volatile Connection connection;

	private int taskNumber;
	private int numTasks;
	private Map<String, Long> dbURLMap; //key是url ,value是最近的不可用时间戳，如果超过5分钟，则会重新变为可用
	private List<String> dbURLArray;
	private String currentDbUrl;

	public SimpleJdbcConnectionProvider(JdbcConnectionOptions jdbcOptions) {
		this.jdbcOptions = jdbcOptions;
	}

	@Override
	public Connection getConnection() throws SQLException, ClassNotFoundException {
		if (connection == null) {

			synchronized (this) {
				if (connection == null) {
					Class.forName(jdbcOptions.getDriverName());
					currentDbUrl = jdbcOptions.getDbURL();
					if (jdbcOptions.getUsername().isPresent()) { //
						connection = DriverManager.getConnection(currentDbUrl, jdbcOptions.getUsername().get(), jdbcOptions.getPassword().orElse(null));
					} else {
						connection = DriverManager.getConnection(currentDbUrl);
					}

				}
			}
		}
		return connection;
	}

	@Override
	public Connection getConnection(int taskNumber, int numTasks) throws SQLException, ClassNotFoundException {
		if (connection == null) {

			//设置全局变量，重新获取连接时可以直接使用
			this.taskNumber = taskNumber;
			this.numTasks = numTasks;

			synchronized (this) {
				if (connection == null) {
					Class.forName(jdbcOptions.getDriverName());

					//解析url ,如果是多个地址，将其放入map, 随后在写入时可以顺序获取
					String dbURL = jdbcOptions.getDbURL();
					if(dbURLMap == null){
						initDbURLMap(dbURL);
					}
					if(dbURLMap != null && dbURLMap.size()>0){
						//从urlMap中获取可用的url
						String url = takeDbURLFromMap(taskNumber, numTasks);
						if(url != null){
							dbURL = url;
						}
					}
					currentDbUrl = dbURL;

					if (jdbcOptions.getUsername().isPresent()) { //
						connection = DriverManager.getConnection(currentDbUrl, jdbcOptions.getUsername().get(), jdbcOptions.getPassword().orElse(null));
					} else {
						connection = DriverManager.getConnection(currentDbUrl);
					}

				}
			}
		}
		return connection;
	}

	private String takeDbURLFromMap(int taskNumber, int numTasks) {
		if(dbURLMap != null){
			int urlSize = dbURLArray.size();

			int index = 0;
			if(urlSize <= numTasks){
				index = taskNumber%urlSize;
			}else{ //url个数多于 task , 走roudrobin模式, index是从 taskNumber->dbURLArray.size -> 0
				index = taskNumber;
			}

			//为防止数组下标越界，重头开始遍历
			if(index >= dbURLArray.size()){
				index = 0;
				taskNumber = 0;
			}
			String dbUrl = dbURLArray.get(index);
			if(dbUrlIsUseful(dbUrl)){
				LOG.info("taskNumber:{}===index and dbUrl---------{}, {}", taskNumber, index, dbUrl);
				return dbUrl;
			}else{
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				taskNumber++;
				String s = takeDbURLFromMap(taskNumber, numTasks);
				if(s != null){
					return s;
				}
			}
		}
		return null;
	}

	private boolean dbUrlIsUseful(String dbUrl){
		if(dbUrl != null){
			Long timestamp = dbURLMap.get(dbUrl);
			if(timestamp == -1L){
				dbURLMap.put(dbUrl, 0L); //将其置为可用
				return false;
			}

			//不可用时间没超过1分钟，跳过该url
			if(timestamp == 0L || (System.currentTimeMillis() - timestamp) > 1*60*1000){
				return true;
			}
		}
		return false;
	}

	private synchronized void initDbURLMap(String dbURL){
		if(dbURL.contains("[") && dbURL.contains("]")){
			dbURLMap = new ConcurrentHashMap<>();
			dbURLArray = new ArrayList<>();

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
		}
	}

	@Override
	public Connection reestablishConnection() throws SQLException, ClassNotFoundException {
		try {
			connection.close();
		} catch (SQLException e) {
			LOG.info("JDBC connection close failed.", e);
		} finally {
			connection = null;
		}

		//设置对应的url为不可用
		dbURLMap.put(currentDbUrl, System.currentTimeMillis());
		LOG.info("taskNumber:{}-------开始重新获取连接", taskNumber);

		connection = getConnection(taskNumber, numTasks);
		return connection;
	}

	@Override
	public Connection reestablishConnection(boolean isTotalCountTooLarge) throws SQLException, ClassNotFoundException {
		try {
			connection.close();
		} catch (SQLException e) {
			LOG.info("JDBC connection close failed.", e);
		} finally {
			connection = null;
		}

		if(isTotalCountTooLarge){
			dbURLMap.put(currentDbUrl, -1L); //-1表示可用，但是建议当前不使用
			LOG.info("taskNumber:{}-------isTotalCountTooLarge, 开始重新获取连接", taskNumber);
		}else{
			//设置对应的url为不可用
			dbURLMap.put(currentDbUrl, System.currentTimeMillis());
			LOG.info("taskNumber:{}-------开始重新获取连接", taskNumber);
		}

		connection = getConnection(taskNumber, numTasks);
		return connection;
	}

}
