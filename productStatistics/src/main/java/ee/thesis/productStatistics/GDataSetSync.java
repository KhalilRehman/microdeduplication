package ee.thesis.productStatistics;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class GDataSetSync {
	public static void main(String[] args){
		if (args.length < 3) {
			System.err.println("Usage: Input <file 1>\nInput <file 2>\nOutput <file>");
			System.exit(1);
		}
	
		// Initialize Spark Context
		SparkConf sparkConf = new SparkConf().setAppName("GSet Synchronization");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
	
		JavaRDD<String> gDataSet = ctx.textFile(args[0], 1)
				/*.map(new Function<String, String>(){
				@Override
				public String call(String arg0) throws Exception {
					String id=arg0.replaceAll("<|>", "").split(";")[0];						
					return id;
				}					
			})*/
			;
		JavaRDD<String> oDataSet = ctx.textFile(args[1], 1);
		List<String> gList=gDataSet.collect();
		
		JavaRDD<String> resultSet=
				oDataSet.map(new Function<String, String>(){
					public String call(String line){
						String id=line.replaceAll("<|>", "").split(";")[0];
						if(gList.contains(id))							
							return line;
						return "";
					}
				}).filter(new Function<String, Boolean>(){
			@Override
			public Boolean call(String arg0) throws Exception {
				if(!arg0.isEmpty())					
						return true;	
				return false;
			}				
	    });
		
		resultSet.saveAsTextFile(args[2]);	
	}
}
