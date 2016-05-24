package ee.thesis.deduplication;

import org.apache.commons.lang.ArrayUtils;

import ee.thesis.processes.ConvertionToEntities;
import ee.thesis.processes.Deduplication;
import ee.thesis.processes.EvaluateDeduplication;

public class App {
	
	public static void main(String[] args){
		if (!(args.length > 0)) {
			System.err.println("Usage: Process <entityformation, deduplication, or evaluation>."
					+ "  \nInput File <file>.\nOutput <file>.");
			System.exit(1);
		}
		
		try{
			if(args[0].trim().toLowerCase().equals("entityformation")){
				args=(String[]) ArrayUtils.removeElement(args, args[0]);
				ConvertionToEntities.convertToEntities(args);
			}
			else if(args[0].trim().toLowerCase().equals("deduplication"))
				Deduplication.Deduplicate(args);
			else if(args[0].trim().toLowerCase().equals("evaluation"))
				EvaluateDeduplication.evaluate(args);
		}catch(Exception ex){
			System.out.println(ex.getMessage());
		}
	}
	
}
