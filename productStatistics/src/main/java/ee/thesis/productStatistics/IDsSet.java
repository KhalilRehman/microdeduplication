package ee.thesis.productStatistics;

import java.util.HashSet;

public class IDsSet{
	private static volatile IDsSet idsSet;
	private static HashSet<Long> idsHashSet;
	
	private IDsSet(){
		idsHashSet=new HashSet<>();
	}
	
	public static IDsSet getInstance() {
        if (idsSet == null ) {
            synchronized (IDsSet.class) {
                if (idsSet == null) {
                    idsSet = new IDsSet();
                }
            }            
        }
        return idsSet;
	}
	
	public static void add(Long id){
		idsHashSet.add(id);
	}
	public static HashSet<Long> getHashSet(){
		return idsHashSet;
	}
}
