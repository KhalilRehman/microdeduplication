package ee.thesis.processSeqFiles.utils;


import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.any23.Any23;
import org.apache.any23.source.DocumentSource;
import org.apache.any23.source.*;
import org.apache.any23.writer.NTriplesWriter;
import org.apache.any23.writer.TripleHandler;

public class MicroDataExtraction {
	List<String> statementsList;
	
	public MicroDataExtraction(List<String> statementsList){
		this.statementsList=statementsList;
	}
	
	public String extractMicroData(String htmlContents) throws Exception{
		
		System.out.println("In extracting microdata.");
		
		Any23 runner = new Any23("html-microdata");
		
		File file=createTempFile(htmlContents); 
		
		DocumentSource source= new FileDocumentSource(file);
		System.out.println("Document source created.");
		
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		TripleHandler handler = new NTriplesWriter(out);
		System.out.println("Triple handler occupied.");
		String result ="";
		      try {
		    	  System.out.println("Extracting microdata.");
		    	  runner.extract(source, handler);
		    	  result = out.toString("UTF-8");
		  		  System.out.println("############################");
		  		  System.out.println(result);
		  		  System.out.println("############################");
		      } 
		      finally {		
		    	 handler.close();		    	 
		      }
		
		
		if(!result.isEmpty())
			return removeDuplicateTriples(result);
		else
			throw new Exception();
		//return result;				
	}

	private String removeDuplicateTriples(String result) {
		String[] tripleStatements=result.split("\\\n");
		Set<String> triplesSet=new LinkedHashSet<String>(Arrays.asList(tripleStatements));
		String uResult="";
		for(String statement:triplesSet){
			uResult+=statement+"\n";
		}
		System.out.println(uResult);
		return uResult;		 
	}

	private File createTempFile(String htmlContents) throws IOException {
		String filePath="data.html";
		File temp = new File(filePath);
		//File temp = File.createTempFile("data", ".html");	
		FileWriter fileWriter=new FileWriter(temp, false);
		fileWriter.write(htmlContents);
	    //BufferedWriter fileWriter = new BufferedWriter(new FileWriter(temp));
	    fileWriter.write(htmlContents);
		fileWriter.flush();
		fileWriter.close();	    
		return temp;
	}

	public void setStatements(String key, String result) {
		String[] statements=result.split("(\\s\\.)(\\r?\\n)");
		StringBuilder stat=null;
		for(String statement: statements){
			stat=new StringBuilder("");
			System.out.println(statement);
			String[] statParts=statement.split("\\s(<|\"|_)");
		
			String subject=statParts[0].replaceAll("(<|>|\")", "");
			String predicate=statParts[1].replaceAll("(<|>|\")", "");
			String object=statParts[2].replaceAll("(<|>|\")", "");
			
			stat.append("<"+key+">, ").append("<"+subject+">, ")
				.append("<"+predicate+">, ").append("<"+object+">");
			statementsList.add(stat.toString());
			//System.out.println(stat.toString());
		}
	}
	
	public List<String> getStatements(){
		return statementsList;
	}

	public void writeToFile(File file, List<String> statList) throws IOException {
		
		//File file=new File(csvFilePath);
		File directory=new File(file.getParent());
		
		if(!directory.exists())
			directory.mkdirs();
		
		FileWriter fileWriter=new FileWriter(file);
		for(String statement:statList){
			fileWriter.write(statement);
			fileWriter.write("\n");
		}
		fileWriter.flush();
		fileWriter.close();	
		
	}

}
