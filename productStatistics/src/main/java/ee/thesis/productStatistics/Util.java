package ee.thesis.productStatistics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Util {
	public static String product;
	public static HashMap<String, String> productMap;
	public static List<ArrayList<Long>> recordsComparisonList=
			new ArrayList<ArrayList<Long>>();
	//public static IDsSet pairIDSet=IDsSet.getInstance();
	public static String clearLine(String line){
		line= line.replaceAll("(\\\\t)|(\\\\n)|(@(et|en|ru|de|ee))|(-(et|en|ru|de|ee))", "");
		String[] tuples=line.split(";");
		String clearedLine="";
		int i=0;
		for(String t:tuples){
			String s=t.replaceAll("<|>", "").trim().replaceAll(" +", " ");
			if(i==0)
				clearedLine+="<"+s+">";
			else
				clearedLine+=";"+"<"+s+">";
			i=1;			
		}
		return clearedLine;
	}

	public static String deCodeLine(String line){
		String encode="";
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
					
					hexString=hexString.replaceAll("<|>", "");
					int hexVal = Integer.parseInt(hexString, 16);
					encode += (char)hexVal;
					encode += s.replace("u"+hexString, "");
				}catch(Exception e){
					encode += s.replace("u"+hexString, "");
					continue;
				}
			}
				else
					encode+=s;
			}   				
		return encode;
	}

	public static String matchPairs(Object[] recordsPairs) {
		String recordToReturn="";
		ArrayList<Long> prePairsIDsList=new ArrayList<Long>();
		if(recordsPairs!=null){	
			List<Product> productList=new ArrayList<Product>();
			for(Object recordObject:recordsPairs){
				String[] record=
						recordObject.toString().replaceAll("<|>", "").split(";");
				Product product=setProduct(record);
				productList.add(product);
			}
			Product product=new Product(); 
			
			int lastRecordIndex=recordsComparisonList.size()-1;
			if(!recordsComparisonList.isEmpty())
				prePairsIDsList=recordsComparisonList.get(lastRecordIndex);
			
			if(prePairsIDsList.size()<2){
				product=productList.get(0);
				recordToReturn=product.toString();		
				
				prePairsIDsList.add(product.getId());
			}else{
				product=productList.get(0);
				if(!(prePairsIDsList.contains(product.id))){
					prePairsIDsList=new ArrayList<>();
					prePairsIDsList.add(product.id);
					recordToReturn=product.toString();
				}
				else{
					recordsComparisonList.remove(lastRecordIndex);
				}
			}
			for(int i=1; i<productList.size(); i++){
				Product productOther=productList.get(i);
				if(!prePairsIDsList.contains(productOther.id)){
					if(product.equals(productOther))
						prePairsIDsList.add(productOther.getId());
					else if(product.compare(productOther))
						prePairsIDsList.add(productOther.getId());
				}
			}
			//Collections.sort(prePairsIDsList);
			recordsComparisonList.add(prePairsIDsList);
			//if(!recordToReturn.isEmpty())
			//	pairIDSet.add(prePairsIDsList.get(0));
		}
		
		/*String pairs="";
		for(Long s: prePairsIDsList){
			pairs+=s+";";
		}*/
		return recordToReturn;
	}

	private static Product setProduct(String[] record) {
		long id = 0;
		String name="", sku="", description="", imageUrl="", prodUrl="", currency="", 
				availability="", provider="", timeStamp="";
		double price=0.0;
		
		Product product=null;
		if(record!=null){
			try{
				id=getLong(record[0]);
				name= getString(record[1]);
				sku=getString(record[2]);
				description=getString(record[3]);
				imageUrl=getString(record[4]);
				prodUrl=getString(record[5]);
				price=getDouble(record[6]);
				currency=getString(record[7]);
				availability=getString(record[8]);
				provider=getString(record[9]);
				timeStamp=getString(record[10]);
			}catch(Exception e){
				//sometimes the record has less field, to handle the exception this step is needed
				//in this method...
			}			
		}
		product= new Product(id, name, sku, description, imageUrl, prodUrl, price, currency, 
				availability, provider, timeStamp);
		return product;		
	}
	private static String getString(String value) {
		String str="";
		if(value!=null && (!(value.isEmpty())))
			str=value;
		return str;
	}

	static long getLong(String value) {
		long id=0;
		try{
			id=Long.parseLong(value);
		}catch(Exception e){
			//id will be zero, handling normal execution in case value is empty or
			// null
		}
		return id;
	}

	static double getDouble(String p) {
		double price=0.0;
		p=p.replaceAll("\\,", ".");
		try{
			price=Double.parseDouble(p.replaceAll("[^\\d.]", ""));
		}catch(Exception e){
			// sometimes the price given is not exactly numeric, to handle the exception this step is needed
			//in this method...
		}
		return price;
	}
	
	static ArrayList<String> getDuplicateCount(){
		ArrayList<String> countList=new ArrayList<>();
		if(recordsComparisonList!=null){
			for(ArrayList<Long> rList:recordsComparisonList){
				String recordCount=rList.get(0).toString();
				recordCount+=";"+rList.size();
				System.out.println(recordCount);
				countList.add(recordCount);
			}				
		}
		return countList;		
	}


}
