package Sparktest.SparkStreaming;



public class App77 {


public static void main(String[] args) {
	HBaseCounterIncrementor incrementor =
            HBaseCounterIncrementor.getInstance(
                "test1",
                "f");
	try {
		incrementor.incerment("Counter", "", 1);
	} catch (Exception e) {
		// TODO 自動生成された catch ブロック
		e.printStackTrace();
	}
  }
}