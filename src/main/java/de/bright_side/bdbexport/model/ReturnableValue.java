package de.bright_side.bdbexport.model;

public class ReturnableValue <K>{
	private K value;
	
	public ReturnableValue(K value){
		this.value = value;
	}
	
	public K getValue() {
		return value;
	}
	
	public void setValue(K value) {
		this.value = value;
	}
}
