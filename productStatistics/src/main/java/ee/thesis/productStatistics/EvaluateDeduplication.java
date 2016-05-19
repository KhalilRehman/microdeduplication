package ee.thesis.productStatistics;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import com.google.common.base.Optional;

import scala.Tuple2;

public class EvaluateDeduplication {	
	public static void main(String[] args) throws Exception {
		if (args.length < 3) {
			System.err.println("Usage: Input <file 1>\nInput <file 2>\nOutput <file>");
			System.exit(1);
		}

		// Initialize Spark Context
		SparkConf sparkConf = new SparkConf().setAppName("Evaluation");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);

		JavaRDD<String> gDataSet = ctx.textFile(args[0], 1)	
				.filter(new Function<String, Boolean>(){
					@Override
					public Boolean call(String line) throws Exception {						
						if(!line.isEmpty()){
							return true;										
						}
						return false;
					}
				})
				.map(new Function<String, String>(){
					@Override
					public String call(String line) throws Exception {
						String[] tuples=line.replaceAll("<|>", "").split(";");
						String name=tuples[1].replaceFirst("^[\\,\\.\\*\\%\\@\\!\\-\\_\\+]\\s", "").trim(), 
								description=tuples[3];						
						return name+description;
					}					
				})
				/*.mapToPair(new PairFunction<String, String, String>() {
	    			public Tuple2<String, String> call(String line) {
	    				String name= line.replaceAll("<|>", "").split(";")[1];
	    				String description= line.replaceAll("<|>", "").split(";")[3];
	    				return new Tuple2<String, String>(name+description, line);
	    			}
				})*/
				;
		JavaRDD<String> oDataSet = ctx.textFile(args[1], 1)
				.filter(new Function<String, Boolean>(){
					@Override
					public Boolean call(String line) throws Exception {						
						if(!line.isEmpty()){
							return true;										
						}
						return false;
					}
				})
				.map(new Function<String, String>(){
					@Override
					public String call(String line) throws Exception {
						String[] tuples=line.replaceAll("<|>", "").split(";");
						String name=tuples[1].replaceFirst("^[\\,\\.\\*\\%\\@\\!\\-\\_\\+]\\s", "").trim(), 
								description=tuples[3];	
						System.out.println(name);
						return name+description;
					}					
				})
				/*.mapToPair(new PairFunction<String, String, String>() {
	    			public Tuple2<String, String> call(String line) {
	    				String name= line.replaceAll("<|>", "").split(";")[1];
	    				String description= line.replaceAll("<|>", "").split(";")[3];
	    				return new Tuple2<String, String>(name+description, line);
	    			}
			})*/
		;
		
		
		double relaventRecords=gDataSet.count();
		double retrievedRecords=oDataSet.count();
		
		Set<String> gSet= new HashSet<>(gDataSet.collect());
		Set<String> oSet= new HashSet<>(oDataSet.collect());
		
		oSet.retainAll(gSet);
		
		long intersectedRecords=oSet.size();
		
		List<String> gList= new ArrayList(gDataSet.collect());		 
		List<String> oList= new ArrayList(oDataSet.collect());
		System.out.println("O Data Set Size: "+ retrievedRecords);
		System.out.println("True Positive: "+intersectedRecords);
		double p=intersectedRecords/retrievedRecords;
		System.out.println("Precision: "+p);
		double r=intersectedRecords/relaventRecords;
		System.out.println("Recall: "+r);
		System.out.println("F-Score: "+(2*(p*r)/(p+r)));
		
				
		
		long gSize=gList.size();
		
		long truePositive=0;
		long falsePositive=0;
		long falseNegative=0;
		for(String name:oList){
			if(gList.contains(name)){
				int index=gList.indexOf(name);
				gList.remove(index);
				truePositive+=1;
			}
			else	
				falsePositive+=1;
		}
		falseNegative=gSize-truePositive;
		double sum= truePositive+falsePositive;
		double precision= truePositive/sum;
		sum=truePositive+falseNegative;
		double recall= truePositive/sum;
		double fScore= 2*((precision*recall)/(precision+recall));
		
		System.out.println("True Positive 2nd: "+ truePositive);
		System.out.println("Precision 2nd: "+precision);
		System.out.println("Recall 2nd: "+ recall);
		System.out.println("F-Score 2nd: "+fScore);
		
		//JavaPairRDD<String, Tuple2<String, String>> resultSet= oDataSet.join(gDataSet);		
		//resultSet.saveAsTextFile(args[2]);			
	}

}
