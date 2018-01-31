package org.apache.storm.firststorm.v096;

import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class RandomSpout extends BaseRichSpout {

	/**
	 * 
	 */
	private static final long serialVersionUID = -9074048335012796027L;
	private SpoutOutputCollector collector;
	private static String[] words = {"Hadoop","Storm","Apache","Linux","Nginx","Tomcat","Spark"};
	
	/**
	 * 把Tuple发送至下游
	 */
	public void nextTuple() {
		String word = words[new Random().nextInt(words.length)];
		collector.emit(new Values(word));
	}

	/**
	 * 数据源初始化
	 */
	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector arg2) {
		this.collector = arg2;
	}

	
	/**
	 * 定义输出字段
	 */
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		arg0.declare(new Fields("randomString"));
	}

}
