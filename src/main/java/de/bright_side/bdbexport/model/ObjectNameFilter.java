package de.bright_side.bdbexport.model;

import java.util.Set;

public class ObjectNameFilter {
	private Set<String> include;
	private Set<String> exclude;
	public Set<String> getInclude() {
		return include;
	}
	public void setInclude(Set<String> include) {
		this.include = include;
	}
	public Set<String> getExclude() {
		return exclude;
	}
	public void setExclude(Set<String> exclude) {
		this.exclude = exclude;
	}
	
	@Override
	public String toString() {
		return "Filter [include=" + include + ", exclude=" + exclude + "]";
	}
	
}
