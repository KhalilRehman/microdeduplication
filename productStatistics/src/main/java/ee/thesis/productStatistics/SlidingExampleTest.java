package ee.thesis.productStatistics;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.rdd.RDDFunctions;
import org.apache.spark.rdd.RDD;

import scala.Tuple2;

public class SlidingExampleTest {
	
	private static long index=0;
	
	public static void main(String[] args){
		if (args.length < 2) {
			System.err.println("Usage: Input <file>\nOutput <file>");
			System.exit(1);
		}

		// Initialize Spark Context
		SparkConf sparkConf = new SparkConf().setAppName("Product Unification");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);

		JavaRDD<String> lines = ctx.textFile(args[0], 16)
				.filter(new Function<String, Boolean>(){
			@Override
			public Boolean call(String line) throws Exception {
				
				if(!line.isEmpty()){
					return true;			
				}
				return false;
			}				
	    });
		
		JavaPairRDD<String, String> keyEntity = 
	    		lines.mapToPair(new PairFunction<String, String, String>() {
	    			public Tuple2<String, String> call(String line) {
	    				String key=generateKey(line);
	    	        	return new Tuple2<String, String>(key, line);
	    	        }

					private String generateKey(String line) {
						String key="";
						line=line.replaceAll("<|>", "");
						String[] tuple=line.split(";");
						try{
								if(tuple!=null && tuple.length>1){
							
									key+=tuple[1];
									key+=tuple[5];																	
							}
						}
						catch(Exception e){							
							System.out.println("Exception: "+e.getMessage()+" "+key);
						}
						return key;
					}
	    		});
		
		JavaRDD<String> sortedEntity=keyEntity.sortByKey().values();
		System.out.println();
		final int windowSize=3;
		RDD<Object> r = RDDFunctions.fromRDD(sortedEntity.rdd(), sortedEntity.classTag()).sliding(windowSize);
		JavaRDD<Object> x = new JavaRDD<>(r, r.elementClassTag());
		
		/*List<Object> resultL=x.collect();
		for(Object record: resultL)
			System.out.println(Arrays.toString((Object[])record));*/
			
		final long size=x.count();
		//Object lastRecord=x.take((int) size);
		//System.out.println(Arrays.toString((Object[])lastRecord));
		//Map the object RDD to String RDD
		JavaRDD<String> result = x.map(new Function<Object,String>() {
			@Override
			public String call(Object recordsPairs) throws Exception {
				//String record= Util.matchPairs((Object[]) recordsPairs);
				index++;
				String records="";				
				if(index==size){	
					Object[] lastPairs= (Object[])recordsPairs;
					for(int i=0; i<lastPairs.length; i++){
						Object[] newArray=new Object[lastPairs.length-i];
						newArray= Arrays.copyOfRange(lastPairs, i, lastPairs.length);
						records+=Arrays.toString(newArray)+"\n";
					}
				}
				else
					records= Arrays.toString((Object[])recordsPairs);
				
				return records;

			}
		});
		ctx.parallelize(result.collect()).saveAsTextFile(args[1]);

	}
}
