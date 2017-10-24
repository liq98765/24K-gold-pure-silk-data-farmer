package com.futuredata.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseTest {



	public static void main(String[] args) throws Exception {
		//创建表
		//createTable();
		//插入数据
		//insertData();
		//获取数据
		getData();
	}

	/**
	 * 创建table
	 * @throws Exception
	 **/
	public static void createTable() throws Exception {
		Configuration cf = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(cf);
		//表存在删除
		if (admin.tableExists("t1")) {
			admin.disableTable("t1");
			admin.deleteTable("t1");
		}
		HTableDescriptor table = new HTableDescriptor(TableName.valueOf("t1"));
		//列簇
		table.addFamily(new HColumnDescriptor("info"));
		//表创建
		admin.createTable(table);
		admin.close();
	}

	/**
	 * 获取table
	 **/
	public static HTable getTable() throws IOException{
		Configuration cf = HBaseConfiguration.create();
		HTable table = new HTable(cf, "test");
		return table;
	}

	/**
	 * 插入数据
	 * @throws Exception
	 **/
	public static void insertData(String rowKey, String key, int increment) throws Exception {
		HTable table = getTable();
		Put put = new Put(Bytes.toBytes(rowKey));
		put.add(Bytes.toBytes("f"), Bytes.toBytes(key), Bytes.toBytes(increment));
		table.put(put);
		table.close();
	}

	/**
	 * 获取数据
	 * @throws Exception
	 **/
	public static void getData() throws Exception {
		HTable table = getTable();
		Get get = new Get(Bytes.toBytes("f:qq"));
		System.out.println("OK!");
		Result result = table.get(get);
		System.out.println("OK77!");
		for (Cell cell : result.rawCells()) {
			System.out.println(Bytes.toString(CellUtil.cloneRow(cell)));
			System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)));
			System.out.println(Bytes.toString(CellUtil.cloneQualifier(cell)));
			System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
		}
		table.close();
	}
}
