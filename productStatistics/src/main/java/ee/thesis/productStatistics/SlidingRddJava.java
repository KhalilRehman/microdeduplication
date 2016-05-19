package ee.thesis.productStatistics;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.rdd.RDDFunctions;
import org.apache.spark.rdd.RDD;

import scala.Tuple2;

public class SlidingRddJava {
	
	private static long index=0;

	public static void main(String[] args) throws Exception {
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
		
				
		final int windowSize=Integer.parseInt(args[2]);//9;
		//final Util util=new Util();
		//Create sliding RDD: Sliding window size is 3 here. 
		RDD<Object> r = RDDFunctions.fromRDD(sortedEntity.rdd(), sortedEntity.classTag()).sliding(windowSize);
		JavaRDD<Object> x = new JavaRDD<>(r, r.elementClassTag());
		final long size=x.count();
		
		//Map the object RDD to String RDD
		JavaRDD<String> result = x.map(new Function<Object,String>() {
			@Override
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
			@Override
			public Boolean call(String arg0) throws Exception {
				return !arg0.isEmpty();
			}
		});			
		
		//ctx.parallelize(result.collect()).saveAsTextFile(args[1]);
		result.saveAsTextFile(args[1]);

		//if((args[3]!=null) || (!(args[3].isEmpty())))
		//	ctx.parallelize(Util.getDuplicateCount()).saveAsTextFile(args[3]);
		
	}
}

/*package ee.thesis.productStatistics;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.rdd.RDDFunctions;
import org.apache.spark.rdd.RDD;

import scala.Tuple2;

public class SlidingRddJava {
	
	private static long index=0;

	public static void main(String[] args) throws Exception {
		if (args.length < 3) {
			System.err.println("Usage: Input <file>\nOutput <file>");
			System.exit(1);
		}
		
		SparkConf sparkConf = new SparkConf().setAppName("Product Unification");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		
		performDeduplication(args, ctx);
		
		if(args[3]!=null || args[3].isEmpty()){
			int executors=Integer.parseInt(args[3]);
			if(executors>1){
				args[0]=args[1];
				performDeduplication(args, ctx);
			}
		}
		// Initialize Spark Context	
		
		
		
		JavaRDD<String> sortedEntity=keyEntity.sortByKey().values();
		
		List<String> list=sortedEntity.collect();
		
		for(String l: list)
			System.out.println(l);
		
		final int windowSize=3;//args[2];
		//final Util util=new Util();
		//Create sliding RDD: Sliding window size is 3 here. 
		RDD<Object> r = RDDFunctions.fromRDD(sortedEntity.rdd(), sortedEntity.classTag()).sliding(windowSize);
		JavaRDD<Object> x = new JavaRDD<>(r, r.elementClassTag());
		
		final long size=x.count();
		//Map the object RDD to String RDD
		x.foreach(new VoidFunction<Object>(){ 
	          public void call(Object recordsPairs) {
	        	  Util.matchPairs((Object[]) recordsPairs);//totalMap.put(yourCSVParser(line)); //this is dummy function call 
	    }});
		JavaRDD<String> result = x.map(new Function<Object,String>() {
			@Override
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
			@Override
			public Boolean call(String record) throws Exception {
				return !record.isEmpty();				
			}
		});
		
		JavaPairRDD<String, String> keyEntities = 
	    		result.mapToPair(new PairFunction<String, String, String>() {
	    			public Tuple2<String, String> call(String line) {
	    				String key=line.replaceAll("<|>", "");
	    	        	return new Tuple2<String, String>(key, line);
	    	        }
	    		});
			
		JavaRDD<String> sortedEntities= keyEntities.sortByKey().values();
		RDD<Object> r1 = RDDFunctions.fromRDD(sortedEntities.rdd(), sortedEntities.classTag()).sliding(windowSize);
		JavaRDD<Object> x1 = new JavaRDD<>(r1, r.elementClassTag());
		JavaRDD<String> uniqueEntities = x.map(new Function<Object,String>() {
			@Override
			public String call(Object recordsPairs) throws Exception {
				String record="";
				record= Util.matchPairs((Object[]) recordsPairs);
				//String record= Util.matchPairs((Object[]) recordsPairs);				
				return record;
			}
		}).filter(new Function<String, Boolean>(){
			@Override
			public Boolean call(String record) throws Exception {
				return !record.isEmpty();				
			}
		});
		//System.out.println(pairs.count());
		JavaRDD<String> result=sortedEntity.filter(new Function<String, Boolean>(){
			@Override
			public Boolean call(String record) throws Exception {
				if(!record.isEmpty()){
					String[] tuples=record.split(";");
					Long id=Util.getLong(tuples[0].replaceAll("<|>", ""));
					if(Util.pairIDSet.contains(id))
						return true;
				}
				return false;
			}
		});
		ctx.parallelize(keyEntity.collect()).saveAsTextFile(args[1]);
		//if((args[3]!=null) || (!(args[3].isEmpty())))
		//	ctx.parallelize(Util.getDuplicateCount()).saveAsTextFile(args[3]);
		
		//HashSet<Long> pairSet=new HashSet<>(Util.);
		//System.out.println("Result: "+ result.count());

		//System.out.println("ID Set "+ Util.pairIDSet.size());
		
		//result.saveAsTextFile(args[1]);
	}

	private static void performDeduplication(String[] args, JavaSparkContext ctx) {		
		JavaRDD<String> lines = ctx.textFile(args[0], 16)
				.filter(new Function<String, Boolean>(){
			@Override
			public Boolean call(String line) throws Exception {
				
				if(!line.isEmpty()){
					//return true;
					Boolean vRecord=isRecordValid(line);
					return 	vRecord;		
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
		
		
	}
}*/