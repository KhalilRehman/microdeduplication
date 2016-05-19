package ee.thesis.productStatistics;

import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
    	if (args.length < 2) {
	        System.err.println("InputFile: NTriples <file>\nOutputFile: File Path");
	        System.exit(1);
	      }

    	SparkConf sparkConf = new SparkConf().setAppName("Count Product Names");
	    JavaSparkContext ctx = new JavaSparkContext(sparkConf);
	    
	    JavaRDD<String> lines = ctx.textFile(args[0], 16).filter( 
	    		/*line -> line.matches("(.*Product/name.*)"));*/
	    		new Function<String, Boolean>(){
	    			public Boolean call(String line){
	    				return line.matches("(.*Product/name.*)");	    				
	    			}
	    });
	    
	    System.out.println("############# Filtered Lines are parallelized!!!!");
	     
	    
	    JavaPairRDD<String, String> clearedLines = 
	    		lines.mapToPair(new PairFunction<String, String, String>() {
	    	//@Override
	    	public Tuple2<String, String> call(String line) {
	    		System.out.println("############# In Clearing....");
	        	line=line.replaceAll("(\\\\t)|(\\\\n)|(@(et|en|ru|de|ee))|(-(et|en|ru|de|ee))", "");
	        	line=decodeUniCode(line);
	        	String name="";
	        	
	        	//if(line.contains("/name"))	        	
	        	name=line.split(">, ")[3];	        	
	        	System.out.println("############# Lines Cleared!!!");
	        	return new Tuple2<String, String>(name, line);
	        }

			private String decodeUniCode(String line) {
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
	      });
	    
	    JavaPairRDD<String, Iterable<String>> grouped=clearedLines.groupByKey();
	    JavaRDD<Tuple2<String, String>> result =
	    		grouped.map(new Function<Tuple2<String, Iterable<String>>, Tuple2<String, String>>(){
	    	
					private static final long serialVersionUID = 1L;

			//@Override
	    	public Tuple2<String, String> call(
	    			Tuple2<String, Iterable<String>> group) throws Exception{
	    		List<String> list=new ArrayList<String>((Collection) group._2);
	    		int count=0;
	    		System.out.println("############# Counting....");
	    		for (String l:list){		    		
		    		if(!l.isEmpty())
		    			count++;
		    	}
	    		String pName=group._1;
	    		return new Tuple2<String, String>(pName, count+"");
	    	}
	    });
	    
	    File file=new File(args[1]);
	    if(file.exists()){
	    	String[] files=file.list();
	    	for(String f: files){
	    		File currentFile = new File(file.getPath(),f);
	    		currentFile.delete();
	    	}
	    	file.delete();	
	    }
	    
	    result.map(new Function<Tuple2<String, String>, String>() {
	    	  public String call(Tuple2<String, String> s) { 
	    		  return s._1 +";" + s._2;}
	    	}).saveAsTextFile(args[1]);
	    
	    
	    ctx.stop();	 
    }
}

// A bit different Style

/*JavaRDD<String> lines = ctx.textFile(args[0], 16).filter( 
line -> line.matches("(.*Product/name.*)"));
new Function<String, Boolean>(){
	public Boolean call(String line){
		return line.matches("(.*Product/name.*)");	    				
	}
});

List<Tuple2<String, Tuple2<Integer, String>>> result = 
lines.mapToPair(new PairFunction<String, String, Tuple2<Integer, String>>() {
//@Override
public Tuple2<String, Tuple2<Integer, String>> call(String line) {
System.out.println("############# In Clearing....");
line=line.replaceAll("(\\\\t)|(\\\\n)|(@(et|en|ru|de|ee))|(-(et|en|ru|de|ee))", "");
line=decodeUniCode(line);
String name="";

//if(line.contains("/name"))	        	
name=line.split(">, ")[3];	        	
System.out.println("############# Lines Cleared!!!");
return new Tuple2<String, Tuple2<Integer, String>>(name, new Tuple2(1, line));
}

private String decodeUniCode(String line) {
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
}).reduceByKey((v1, v2)-> (new Tuple2(v1._1+v2._1, v1._2.split(">, ")[0]+";"+v2._2.split(">, ")[0]))).collect();

ctx.parallelize(result).saveAsTextFile(args[1]);*/
