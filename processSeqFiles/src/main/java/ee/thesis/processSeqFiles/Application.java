package ee.thesis.processSeqFiles;
import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FilenameUtils;

import ee.thesis.processSeqFiles.models.KeyValue;
import ee.thesis.processSeqFiles.utils.FilesUtil;
import ee.thesis.processSeqFiles.utils.MicroDataExtraction;



public class Application {
	
	public static void main(String[] args){
		MicroDataExtraction microDataExtraction;
		List<KeyValue> keyValueList;
		List<String> statementList;
		
		if(args.length<2){
			System.out.println("Please enter paths to read and write file(s)");
			return;
		}
			
    	String seqFileDirectoryPath=args[0];//"D:\\Docs\\Thesis\\SequenceFiles";
		String nTriplesDirectoryPath=args[1];//"D:\\Docs\\Thesis\\NTriples";
    	
		File seqFileDirectory=new File(seqFileDirectoryPath);
		try{
			int fileCount=0;
			File[] filesList=seqFileDirectory.listFiles();
			String csvFilePath;
			File csvFile;
			//for (File seqFile : filesList) {
			for(int k=0; k<filesList.length; k++){
				File seqFile=filesList[k];
				
				csvFilePath=new StringBuilder(nTriplesDirectoryPath)
						.append("\\").append(seqFile.getName().toString())
						.append(".csv").toString();
				csvFile=new File(csvFilePath);
				if(csvFile.exists()){
					System.out.println("Already Exist: file://"+csvFilePath);
					continue;
				}
				String filePath=seqFile.getPath();
				String fileExtension= FilenameUtils.getExtension(filePath);
				System.out.println(fileExtension);
				if(!fileExtension.equals("seq"))
					continue;
				microDataExtraction=new MicroDataExtraction(new ArrayList<String>());
				statementList=new ArrayList<String>();
				keyValueList=null;
				try{
					keyValueList=FilesUtil.readSequenceFile(filePath);
					
				}catch(Exception e){
					continue;
				}
				fileCount++;
				
				//else{
					for(int i=0; i<keyValueList.size(); i++){
						KeyValue keyValue=keyValueList.get(i);
						try{
							String nTriples=
								microDataExtraction.extractMicroData(keyValue.getValue());
						
							microDataExtraction.setStatements(keyValue.getKey(), nTriples);
						}catch(Exception e){
							System.out.println(e.toString());
							continue;
						}
					}
					statementList=microDataExtraction.getStatements();					
					System.out.println(csvFilePath);
					microDataExtraction.writeToFile(csvFile, statementList);
				//}
			System.out.println("Total Sequence Files Read: "+fileCount);
		    }
		}catch(Exception e){
			System.out.println(e.toString());
		}
	}

}
