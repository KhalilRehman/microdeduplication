package ee.thesis.models;

import ee.thesis.utils.CosineSimilarity;

public class Product {
	
	long id;
	String name;
	String sku;
	String description;
	String imageUrl;
	String prodUrl;
	double price;
	String currency;
	String provider;
	String timeStamp;
	String availability;
	
	public Product(){
		this.id=0;
		this.name="";
		this.sku="";
		this.description="";
		this.imageUrl="";
		this.prodUrl="";
		this.price=0.0;
		this.currency="";
		this.provider="";
		this.timeStamp="";
		this.availability="";	
	}
	
	public Product(long id, String name, String sku, String description, String imageUrl, String prodUrl, 
			double price, String currency, String availability, String provider, String timeStamp){
		this.id=id;
		this.name=name;
		this.sku=sku;
		this.description=description;
		this.imageUrl=imageUrl;
		this.prodUrl=prodUrl;
		this.price=price;
		this.currency=currency;
		this.provider=provider;
		this.timeStamp=timeStamp;
		this.availability=availability;		
	}

	public long getId() {
		return id;
	}

	public String getName() {
		return name;
	}

	public String getSku() {
		return sku;
	}

	public String getDescription() {
		return description;
	}

	public String getImageUrl() {
		return imageUrl;
	}

	public String getProdUrl() {
		return prodUrl;
	}

	public double getPrice() {
		return price;
	}

	public String getCurrency() {
		return currency;
	}

	public String getProvider() {
		return provider;
	}

	public String getTimeStamp() {
		return timeStamp;
	}

	public String getAvailability() {
		return availability;
	}
	
	@Override 
	public boolean equals(Object other) {
		boolean result=false;
        boolean nam = false;
        boolean pro=false;
        boolean pri=false;
        if (other instanceof Product) {
            Product that = (Product) other; 
            nam= (this.getName().equals(that.getName()))? true:false;
            if(!(this.getProvider().isEmpty() || that.getProvider().isEmpty())){
            	pro=(this.getProvider().split("\\/")[0].equals(that.getProvider().split("\\/")[0]))? true:false;
            }
            if(!(this.price==0.0 || that.price==0.0))
            	pri=(this.getPrice()==that.getPrice())? true:false;
            else
            	pri=true;
            result = (nam && pro && pri) ? true:false;        
            
        }
        return result;
    }
	
	public boolean compare(Product other) {
		boolean result=false;
        double nameSim = 0;
        double descSim=0;
        
        if (other instanceof Product) {
            Product that = (Product) other; 
            if(!(this.imageUrl.isEmpty()||that.imageUrl.isEmpty()))
            	if(this.imageUrl.equals(that.imageUrl) && 
            			this.getProvider().split("\\/")[0].equals(that.getProvider().split("\\/")[0]))
            		return true;
            
            nameSim = CosineSimilarity.cosineSimilarity(this.name, that.name);
            if(nameSim == 1.0){
            	if((this.getDescription().isEmpty() || that.getDescription().isEmpty()) &&
                		(this.getImageUrl().isEmpty() && that.getImageUrl().isEmpty()))
            		result= true;            	
            }
            else if(nameSim > 0.5){
            	descSim=CosineSimilarity.cosineSimilarity(this.getDescription(), that.getDescription());
            	if(descSim>0.7)
            		result= true;
            }       
            
        }
        return result;
	}
	
	@Override
	public String toString(){
		return "<"+this.getId()+">;"+"<"+this.getName()+">;"+"<"+this.getSku()+">;"+"<"+this.getDescription()+">;"+
				"<"+this.getImageUrl()+">;"+"<"+this.getProdUrl()+">;"+"<"+this.getPrice()+">;"+
				"<"+this.getCurrency()+">;"+"<"+this.getAvailability()+">;"+"<"+this.getProvider()+">;"+
				"<"+this.getTimeStamp()+">;";
	}	
	
	//some old code
	/*double nameSim=0.0;
    boolean nameSimB=false;
    
	com.aliasi.util.Distance<CharSequence> D1 = new EditDistance(false);
    nameSim=D1.distance(this.getName(), that.getName());            	
    if((nameSim/((this.getName().length()+that.getName().length())/2)*100)<40) 
    	nameSimB=true;
    else
    	nam= (this.getName().equals(that.getName()))? true:false;
    if(!(this.getProvider().isEmpty() || that.getProvider().isEmpty())){
    	pro=(this.getProvider().split("\\/")[0].equals(that.getProvider().split("\\/")[0]))? true:false;
    }
    if(!(this.price==0.0 || that.price==0.0))
    	pri=(this.getPrice()==that.getPrice())? true:false;
    else
    	pri=true;
    //tim=(this.getTimeStamp().equals(that.getTimeStamp()))?true:false;   
    //img=this.getImageUrl().equals(that.getImageUrl())?true:false;
    //url=this.getProdUrl().equals(that.getProdUrl())?true:false;
    result = (nam || nameSimB);//(nam && pro && pri && tim && img && url) ? true:false;*/
}
