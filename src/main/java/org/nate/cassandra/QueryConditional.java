package org.nate.cassandra;

public enum QueryConditional {
	
	EQUAL("equal"),
	GREATER_THAN("greaterThan"),
	GREATER_THAN_EQUAL_TO("greaterThanEqual"),
	LESS_THAN_EQUAL_TO("lessThanEqual"),
	LESS_THAN("lessThan"),
	NOT_EQUAL("notEqual");
	
	private String conditional;
	
	QueryConditional(String conditional) {
		this.conditional = conditional;
	}
}
