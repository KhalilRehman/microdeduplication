package ee.thesis.productStatistics;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class SortEntities {
	
	public static void main(String args[]){
		if (args.length < 1) {
			System.err.println("Usage: Input <file>");
			System.exit(1);
		}

		// Initialize Spark Context
		SparkConf sparkConf = new SparkConf().setAppName("Product Unification");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		
		JavaRDD<String> lines = ctx.textFile(args[0], 1)
				.filter(new Function<String, Boolean>(){
			@Override
			public Boolean call(String arg0) throws Exception {
				
				if(!arg0.isEmpty()){
					arg0=arg0.replaceAll("(<|>)|(\")", "");
					String[] str=arg0.split(";");
					try{
						String name="";
						name=str[1];
						if(name.isEmpty())
							return false;
						double price=Double.parseDouble(str[6]);
						if(! (price>0.0))
							return false;
						if(str[2].isEmpty()&&str[3].isEmpty()&&(str[4].isEmpty()||str[5].isEmpty())&&str[7].isEmpty())
							return false;
					}catch(Exception e){
						return false;
					}										
				}
				return true;
			}				
	    });
		
		JavaRDD<String> sortedEntities =
				lines.mapToPair(new PairFunction<String, String, String>() {
	    			public Tuple2<String, String> call(String line) {
	    				line=line.replaceAll("(<|>)|(\")", "");
						String[] str=line.split(";");
						String name="";
						name=str[1];
	    	        	return new Tuple2<String, String>(name, line);
	    			}
	    	    }).sortByKey(false).values();
		sortedEntities.saveAsTextFile(args[1]);
	}

}