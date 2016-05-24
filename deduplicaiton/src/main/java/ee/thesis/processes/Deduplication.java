package ee.thesis.processes;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.rdd.RDDFunctions;
import org.apache.spark.rdd.RDD;

import ee.thesis.utils.Util;
import scala.Tuple2;

public class Deduplication {
	
	private static long index=0;

	public static void Deduplicate(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("Usage: Input <file>\nOutput <file>");
			System.exit(1);
		}

		// Initialize Spark Context
		SparkConf sparkConf = new SparkConf().setAppName("Product Unification");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);

		JavaRDD<String> lines = ctx.textFile(args[0], 16)
				.filter(new Function<String, Boolean>(){
			//@Override
			public Boolean call(String line) throws Exception {
				
				if(!line.isEmpty()){
					//return true;
					return isRecordValid(line);			
				}
				return false;

			}

			private Boolean isRecordValid(String line) {
				String[] str=line.split(";");
				String name="";
				try{
				name=str[1].replaceAll("<|>", "");
				if(name.isEmpty())
					return false;	
				
				double price=Util.getDouble(str[6]);
				if(!(price>0.0))
					return false;					
				if(str[2].isEmpty()&&str[3].isEmpty()&&(str[4].isEmpty()||str[5].isEmpty())&&str[7].isEmpty())
					return false;	
				}catch(Exception e){
					return false;
				}
				return true;
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
									String name=tuple[1];
									//if(name.length()<30)
										key+=name;
									//else
									//	key+=name.substring(0,30);
									 
									key+=tuple[6];
									String provider=tuple[9];	
									if(!(provider.isEmpty())){
										key+=provider.split("\\.")[1];
									}															
							}
						}
						catch(Exception e){							
							System.out.println("Exception: "+e.getMessage()+" "+key);
						}
						return key;
					}
	    		});
		
		JavaRDD<String> sortedEntity=keyEntity.sortByKey().values();	
		
				
		final int windowSize=Integer.parseInt(args[2]);
		
		//Create sliding RDD: Sliding window size is 3 here. 
		RDD<Object> r = RDDFunctions.fromRDD(sortedEntity.rdd(), sortedEntity.classTag()).sliding(windowSize);
		JavaRDD<Object> x = new JavaRDD<Object>(r, r.elementClassTag());
		final long size=x.count();
		
		//Map the object RDD to String RDD
		JavaRDD<String> result = x.map(new Function<Object,String>() {
			//@Override
			public String call(Object recordsPairs) throws Exception {
				index++;
				String record="";
				if(index==size){	
					Object[] lastPairs= (Object[])recordsPairs;
					for(int i=0; i<lastPairs.length; i++){
						Object[] newArray=new Object[lastPairs.length-i];
						newArray= Arrays.copyOfRange(lastPairs, i, lastPairs.length);
						record+=Util.matchPairs(newArray)+"\n";
					}
				}
				else
					record= Util.matchPairs((Object[]) recordsPairs);
				//String record= Util.matchPairs((Object[]) recordsPairs);				
				return record;
			}
		}).filter(new Function<String, Boolean>(){
			//@Override
			public Boolean call(String arg0) throws Exception {
				return !arg0.isEmpty();
			}
		});			
		result.saveAsTextFile(args[1]);
		
	}
}