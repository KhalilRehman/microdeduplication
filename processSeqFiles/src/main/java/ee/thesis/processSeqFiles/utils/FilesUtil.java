package ee.thesis.processSeqFiles.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.any23.extractor.ExtractionException;
import org.apache.any23.writer.TripleHandlerException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import ee.thesis.processSeqFiles.models.KeyValue;



public class FilesUtil {
	
	public static List<KeyValue> readSequenceFile(String seqFilePath) 
			throws IOException, ExtractionException, TripleHandlerException, 
			InstantiationException, IllegalAccessException, Exception{
		
		SequenceFile.Reader reader=null;
		KeyValue keyValue=null;
		List<KeyValue> keyValueList=new ArrayList<KeyValue>();
		System.out.println("Reading Sequence File.");
		try{					
			Configuration config = new Configuration();
			Path path = new Path("file:///"+seqFilePath);
			reader = new SequenceFile.Reader(FileSystem.get(config), path, config);
			WritableComparable<?> key = (WritableComparable<?>) reader.getKeyClass().newInstance();
			Writable value = (Writable) reader.getValueClass().newInstance();
			while (reader.next(key, value)){
				keyValue=new KeyValue();
				//if(value.toString().contains("schema.org/Product")){
					keyValue.setKey(key.toString());
					keyValue.setValue(value.toString());
					keyValueList.add(keyValue);					
				//}			
			}
			System.out.println("Sequence file is read.");
			return keyValueList;
		}catch(Exception e){
			throw e;
		}
		finally{
			if(reader!=null){
				reader.close();
			}
		}		
	}
}
