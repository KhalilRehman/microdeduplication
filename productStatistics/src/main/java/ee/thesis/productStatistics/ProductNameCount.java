package ee.thesis.productStatistics;

import java.util.Iterator;
import java.util.List;

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
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class ProductNameCount {
	
	public static void main(String[] args){
		
		if (args.length < 2) {
	        System.err.println("InputFile: NTriples <file>\nOutputFile: File Path");
	        System.exit(1);
	      }
		
		
		SparkConf sparkConf = new SparkConf().setAppName("Count Product Names");
	    JavaSparkContext ctx = new JavaSparkContext(sparkConf);
	    
		/* Read File with File Name*/		
		JavaPairRDD<LongWritable, Text> javaPairRDD = ctx.newAPIHadoopFile
				(
				args[0], 
				TextInputFormat.class, 
				LongWritable.class, 
				Text.class, 
				new Configuration()
		);

		JavaNewHadoopRDD<LongWritable, Text> hadoopRDD = (JavaNewHadoopRDD) javaPairRDD;
			
		JavaRDD<Tuple2<String, String>> namedLines=
				hadoopRDD.mapPartitionsWithInputSplit(
						new Function2<InputSplit, Iterator<Tuple2<LongWritable, Text>>, 
						Iterator<Tuple2<String, String>>>(){
							private static final long serialVersionUID = 1L;

								@Override
								public Iterator<Tuple2<String, String>> call(InputSplit inSplit,
										final Iterator<Tuple2<LongWritable, Text>> lines) throws Exception {
									FileSplit fileSplit = (FileSplit) inSplit;
									final String fileName=fileSplit.getPath().getName();
									return new Iterator<Tuple2<String, String>>(){
										@Override
						                public boolean hasNext() {
						                    return lines.hasNext();
						                }
						                @Override
						                public Tuple2<String, String> next() {
						                    Tuple2<LongWritable, Text> entry = lines.next();
						                    return new Tuple2<String, String>(fileName, entry._2.toString());
						                }
						                
						                @Override
						                public void remove() {
						                    throw new UnsupportedOperationException();
						                }
									};
								}
							}, true).filter( 
						    		/*line -> line.matches("(.*Product/name.*)"));*/
						    		new Function<Tuple2<String, String>, Boolean>(){
						    			public Boolean call(Tuple2<String, String> nLine){						    				
						    				return nLine._2.matches("(.*Product/name.*)");	    				
						    			}
						    });
		
		JavaPairRDD<String, Tuple2<Integer, String>> result=namedLines.mapToPair(new PairFunction<Tuple2<String, String>, String, Tuple2<Integer, String>>() {

			@Override
			public Tuple2<String, Tuple2<Integer, String>> call(Tuple2<String, String> nLine) throws Exception {
				String fileName=nLine._1;
				String line=nLine._2;
				line=line.replaceAll("(\\\\t)|(\\\\n)|(@(et|en|ru|de|ee))|(-(et|en|ru|de|ee))", "");
	        	line=decodeUniCode(line);
	        	String name="";
	        		        	
	        	name=line.split(">, ")[3];
	        	if(!name.isEmpty())
	        		name=name.replaceAll("(\\<)|(\\>)", "");
	        	System.out.println("############# Lines Cleared!!!");
	        	return new Tuple2<String, Tuple2<Integer, String>>(
	        			name, new Tuple2<Integer, String>(1, fileName));
			}
		})
		.reduceByKey(new Function2<Tuple2<Integer, String>, Tuple2<Integer, String>, 
				Tuple2<Integer, String>>(){
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Integer, String> call(Tuple2<Integer, String> value1, Tuple2<Integer, String> value2)
							throws Exception {
						return new Tuple2<Integer, String>(value1._1+value2._1, value1._2+", "+value2._2);
					}			
		});
		
		List<String> list=result.map(new Function<Tuple2<String, Tuple2<Integer, String>>, String>(){

			@Override
			public String call(Tuple2<String, Tuple2<Integer, String>> arg0) throws Exception {
				
				return new StringBuilder(arg0._1).append(";").append(arg0._2._1).append(";")
						.append(arg0._2._2).toString();
			}
			
		}).collect();
		ctx.parallelize(list).saveAsTextFile(args[1]);
	}

	protected static String decodeUniCode(String line) {
		String encode="";
		System.out.println("############# Decoding...!!!");
		String[] sArray=line.split("\\\\");

		for (String s: sArray){
			if(s.trim().isEmpty())
				continue;
			char ch=s.charAt(0);
			if(ch == 'u'){
				String hexString="";
				try{
					if(s.length()>4)
						hexString=s.substring(1, 5);
					else
						hexString=s.substring(1, s.length());
										
					System.out.println("@@@@HEXSTring "+hexString);
					int hexVal = Integer.parseInt(hexString, 16);
					encode += (char)hexVal;
					encode += s.replace("u"+hexString, "");
				}catch(Exception e){
					System.out.println("######Exception "+e.getMessage()+" "+hexString);
					continue;
				}
			}
				else
					encode+=s;
			}   				
		return encode;
	}

}
