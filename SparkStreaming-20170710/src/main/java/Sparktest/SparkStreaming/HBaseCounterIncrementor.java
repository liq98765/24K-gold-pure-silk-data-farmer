package Sparktest.SparkStreaming;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import Sparktest.SparkStreaming.CounterMap.Counter;

public class HBaseCounterIncrementor {

  static HBaseCounterIncrementor singleton;
  static String tableName;
  static String columnFamily;
  static HTable hTable;
  static long lastUsed;
  static long flushInterval;
  static CloserThread closerThread;
  static FlushThread flushThread;
  static HashMap<String, CounterMap> rowKeyCounterMap =
      new HashMap<String, CounterMap>();
  static Object locker = new Object();

  private HBaseCounterIncrementor(String tableName, String columnFamily) {
    HBaseCounterIncrementor.tableName = tableName;
    HBaseCounterIncrementor.columnFamily = columnFamily;
  }

  public static HBaseCounterIncrementor getInstance(String tableName,
      String columnFamily) {

    if (singleton == null) {
      synchronized (locker) {
        if (singleton == null) {
            singleton = new HBaseCounterIncrementor(tableName, columnFamily);
            initialize();
        }
      }
    }
    return singleton;
  }

  private static void initialize() {
    if (hTable == null) {
      synchronized (locker) {
        if (hTable == null) {
            Configuration hConfig = HBaseConfiguration.create();
            try {
              hTable = new HTable(hConfig, tableName);
              updateLastUsed();

            } catch (IOException e) {
              throw new RuntimeException(e);
            }
            flushThread = new FlushThread(flushInterval);
            flushThread.start();
            closerThread = new CloserThread();
            closerThread.start();
        }
      }
    }
  }

	public static void createTable() throws Exception {
		Configuration cf = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(cf);
		//表存在删除
		if (admin.tableExists("t2")) {
			admin.disableTable("t2");
			admin.deleteTable("t2");
		}
		HTableDescriptor table = new HTableDescriptor(TableName.valueOf("t2"));
		//列簇
		table.addFamily(new HColumnDescriptor("info"));
		//表创建
		admin.createTable(table);
		admin.close();
	}

	public static HTable getTable() throws IOException{
		Configuration cf = HBaseConfiguration.create();
		HTable table = new HTable(cf, "t2");
		return table;
	}

  public void incerment(String rowKey, String key, int increment) throws Exception{
    //incerment(rowKey, key, (long) increment);
	  createTable();
	   hTable = getTable();
		Put put = new Put(Bytes.toBytes(rowKey));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("id11"), Bytes.toBytes("001"));
		try {
			hTable.put(put);
			hTable.close();
		} catch (IOException e) {
			// TODO 自動生成された catch ブロック
			e.printStackTrace();
		}

  }

  public void incerment(String rowKey, String key, long increment) {
    CounterMap counterMap = rowKeyCounterMap.get(rowKey);
    if (counterMap == null) {
      counterMap = new CounterMap();
      rowKeyCounterMap.put(rowKey, counterMap);
    }
    counterMap.increment(key, increment);

    initialize();
  }

  private static void updateLastUsed() {
    lastUsed = System.currentTimeMillis();
  }

  protected void close() {
    if (hTable != null) {
      synchronized (locker) {
        if (hTable != null) {
          if (hTable != null && System.currentTimeMillis() - lastUsed > 30000) {
            flushThread.stopLoop();
            flushThread = null;
            try {
              hTable.close();
            } catch (IOException e) {
              // TODO Auto-generated catch block
              e.printStackTrace();
            }

            hTable = null;
          }
        }
      }
    }
  }

  public static class CloserThread extends Thread {

    boolean continueLoop = true;

    @Override
    public void run() {
      while (continueLoop) {

        if (System.currentTimeMillis() - lastUsed > 30000) {
          singleton.close();
          break;
        }

        try {
          Thread.sleep(60000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }

    public void stopLoop() {
      continueLoop = false;
    }
  }

  protected static class FlushThread extends Thread {
    long sleepTime;
    boolean continueLoop = true;

    public FlushThread(long sleepTime) {
      this.sleepTime = sleepTime;
    }

    @Override
    public void run() {
      while (continueLoop) {
        try {
          flushToHBase();
        } catch (IOException e) {
          e.printStackTrace();
          break;
        }

        try {
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }

    private void flushToHBase() throws IOException {
      synchronized (hTable) {
        if (hTable == null) {
          initialize();
        }
        updateLastUsed();

        for (Entry<String, CounterMap> entry : rowKeyCounterMap.entrySet()) {
          CounterMap pastCounterMap = entry.getValue();
          rowKeyCounterMap.put(entry.getKey(), new CounterMap());

          Increment increment = new Increment(Bytes.toBytes(entry.getKey()));

          boolean hasColumns = false;
          for (Entry<String, Counter> entry2 : pastCounterMap.entrySet()) {
            increment.addColumn(Bytes.toBytes(columnFamily),
                Bytes.toBytes(entry2.getKey()), entry2.getValue().value);
            hasColumns = true;
          }
          if (hasColumns) {
            updateLastUsed();
            hTable.increment(increment);
          }
        }
        updateLastUsed();
      }
    }

    public void stopLoop() {
      continueLoop = false;
    }
  }

}