package ee.thesis.productStatistics;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaNewHadoopRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;
import scala.Tuple6;

public class NTriplesToEntity {
	private static long count=1;
	@SuppressWarnings("serial")
	public static void main(String[] args){		
		if (args.length < 2) {
	        System.err.println("InputFile: NTriples <file>\nOutputFile: File Path");
	        System.exit(1);
	      }
		
		
		SparkConf sparkConf = new SparkConf().setAppName("NTriples To Entity");
	    JavaSparkContext ctx = new JavaSparkContext(sparkConf);
	    
	    Configuration conf=new Configuration(ctx.hadoopConfiguration());
	    conf.set("textinputformat.record.delimiter", "22-rdf-syntax-ns#type>, ");
	    
	    JavaPairRDD<LongWritable, Text> lines= ctx.newAPIHadoopFile
				(
				args[0], 
				TextInputFormat.class, 
				LongWritable.class, 
				Text.class, 
				conf
		);
	    JavaRDD<String> bLines=lines.map(new Function<Tuple2<LongWritable, Text>, String>(){
			@Override
			public String call(Tuple2<LongWritable, Text> line) throws Exception {
				return line._2.toString();
			}	    	
	    }).filter( 
	    		/*line -> line.matches("(.*Product/name.*)"));*/
	    		new Function<String, Boolean>(){
	    			public Boolean call(String bLines){						    				
	    				return (bLines.contains("org/Product")|| bLines.contains("org/Offer"));	    				
	    			}
	    });
	    
	    JavaRDD<String> entity=bLines.map(new Function<String, String>(){

			@Override
			public String call(String bLines) throws Exception {
				
				String name="";
				String sku="";
				String description="";
				String imageUrl="";
				String productUrl="";
				String provider="";
				String price="";
				String currency="";
				String timeStamp="";
				String availability="";
				String[] lines=bLines.split("\n");				
				for(String line: lines){
					try{
						if(bLines.contains(".org/Offer>")){
							if(Util.productMap!=null){
								name=Util.productMap.get("name");
								sku=Util.productMap.get("sku");
								description=Util.productMap.get("description");
								imageUrl=Util.productMap.get("sku");
								productUrl=Util.productMap.get("productUrl");
								provider=Util.productMap.get("provider");
								price=Util.productMap.get("price");
								currency=Util.productMap.get("currency");
								timeStamp=Util.productMap.get("timeStamp");
								availability=Util.productMap.get("availability");
							}
							Util.productMap=null;
						}
						if(line.contains("Product/name")||line.contains("Offer/itemOffered")){
							if(name.isEmpty())
								name=line.split(">, ")[3];
							if(timeStamp.isEmpty())
								timeStamp=getTimeStamp(line);
							if(provider.isEmpty())
								provider=getProvider(line);
						}
						
						else if(line.contains("Product/image") || line.contains("Offer/image"))
								imageUrl=line.split(">, ")[3];
						else if(line.contains("Offer/price")||line.contains("Product/price")){
							if(price.isEmpty())
								price=line.split(">, ")[3];
							if(timeStamp.isEmpty())
								timeStamp=getTimeStamp(line);
							if(provider.isEmpty())
								provider=getProvider(line);
						}
						else if(line.contains("Offer/priceCurrency")||line.contains("Offer/currency") 
								|| line.contains("Product/currency")){
							if(currency.isEmpty())
								currency=line.split(">, ")[3];
						}
						else if(line.contains("Product/description") || line.contains("Offer/description")){
							if(description.isEmpty())
								description = line.split(">, ")[3];
						}
						else if(line.contains("Product/url"))
							productUrl = line.split(">, ")[3];
						else if(line.contains("Product/sku"))
							sku=line.split(">, ")[3];
						else if(line.contains("Offer/availability"))
							availability = line.split(">, ")[3];
					}catch(Exception e){
						continue;
					}
				}	
				String tuple="";
				if(bLines.contains(".org/Product>")){
					HashMap<String, String> pMap=new HashMap<String, String>();
					pMap=new HashMap<String, String>();
					pMap.put("name", name);
					pMap.put("sku", sku);
					pMap.put("description", description);
					pMap.put("imageUrl", imageUrl);
					pMap.put("productUrl", productUrl);
					pMap.put("price", price);
					pMap.put("currency", currency);
					pMap.put("availability", availability);
					pMap.put("provider", provider);
					pMap.put("timeStamp", timeStamp);
					Util.productMap=(HashMap<String, String>) pMap;
				}
				else{
					tuple= new StringBuilder((count++)+"").append(";").append(name).append(";").append(sku).append(";").append(description)
							.append(";").append(imageUrl).append(";").append(productUrl)
							.append(";").append(price).append(";").append(currency)
							.append(";").append(availability)
							.append(";").append(provider).append(";").append(timeStamp)
							.toString().trim();
				
					if(tuple.replaceAll(";", "").trim().isEmpty())
						tuple="";
					else{
						tuple=Util.clearLine(tuple);
						tuple=Util.deCodeLine(tuple);
					}
				}
				return tuple;
			}

			private String getProvider(String line) {
				String provider="";
				try{
					String[] details=line.split(">, ")[0].split("::");
					provider=details[0]+details[1];
				}catch(Exception e){
					//Nothing is required; this is necessary for normal execution if line is empty/incomplete
				}				
				return provider;
			}

			private String getTimeStamp(String line) {
				String timeStamp="";
				try{
					String[] details=line.split(">, ")[0].split("::");
					timeStamp=details[2];
				}catch(Exception e){
					//Nothing is required; this is necessary for normal execution if line is empty/incomplete
				}				
				return timeStamp;
			}
	    	
	    }).filter(new Function<String, Boolean>(){

			@Override
			public Boolean call(String arg0) throws Exception {
				return !arg0.isEmpty();
			}	    	
	    });
	    
	    entity.saveAsTextFile(args[1]);		
	}

}