package ee.thesis.productStatistics;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import scala.Tuple3;

public class CurrencyStats {
	
	public static void main(String[] args){
		if (args.length < 1) {
			System.err.println("Usage: ConceptBasedSearch <file>");
			System.exit(1);
		}

		// Initialize Spark Context
		SparkConf sparkConf = new SparkConf().setAppName("Currency Stats");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);

		JavaRDD<String> lines = ctx.textFile(args[0], 16)
				.filter(new Function<String, Boolean>(){
			@Override
			public Boolean call(String line) throws Exception {
				boolean lineOk=false;
				if( !line.isEmpty()){
					lineOk=true;
					String[] arr=line.split(";");
					try{
						if(arr[0]==null || arr[0].isEmpty())
							lineOk=false;						
					}catch(Exception e){
						return false;						
					}					
				}
				return lineOk;
			}	    	
	    });
		
		JavaRDD<String> result = 
	    		lines.mapToPair(new PairFunction<String, Tuple2<String, String>, Integer>() {
	    			public Tuple2<Tuple2<String, String>, Integer> call(String line) {
	    				
	    				String[] strArray=line.split(";");
	    				String price="";
	    				String currency="";
	    				try{
		    				price=strArray[1];
		    				String priceValue=price.replaceAll("\\,\\s", ".");
		    				priceValue=priceValue.replaceAll("[^\\d.]", "");
		    				if(!(priceValue==null || priceValue.isEmpty()))
		    					priceValue=castToDouble(priceValue);
		    				String priceSymbol=price.replaceAll("([^\\u20AC\\u0024\\u00A3])", "");
		    				price=priceValue+priceSymbol;
			    			currency=strArray[2];
			    			currency=currency.replaceAll("([^a-zA-Z]+)", "").toUpperCase();
			    			String[] currencies={"EUR", "USD", "GBP"};
			    			if(!Arrays.asList(currencies).contains(currency))
			    				currency="NKnown";
	    				}catch(Exception e){
	    					System.out.println(e.getMessage());
	    				}
	    	        	return new Tuple2<Tuple2<String, String>, Integer>(new Tuple2(currency, price), 1);
	    	        }

					private String castToDouble(String priceValue) {
						String value="";
						try{
							if(Double.parseDouble(priceValue)>0)
								value="1";
							return value;
						}catch(Exception e){
							return "";
						}
					}
	    		}).reduceByKey(new Function2<Integer, Integer, Integer>(){

			@Override
			public Integer call(Integer value1,  Integer value2)
					throws Exception {				
				return value1+value2;
			}
			
		}).map(new Function<Tuple2<Tuple2<String, String>, Integer>, String>(){

			@Override
			public String call(Tuple2<Tuple2<String, String>, Integer> arg0) throws Exception {
				
				return arg0._1._1+", "+arg0._1._2+";"+arg0._2;
			}			
		});
		
		ctx.parallelize(result.collect()).saveAsTextFile(args[1]);
	}

}
