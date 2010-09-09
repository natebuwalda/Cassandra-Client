package org.nate.cassandra;

import org.nate.cassandra.annotation.Column;
import org.nate.cassandra.annotation.ColumnFamily;
import org.nate.cassandra.annotation.Key;

@ColumnFamily(name="Standard1")
public class StandardColumnTestClass {
	
	@Key
	private String key;
	
	@Column
	private String aStringColumn;
	
	@Column
	private Integer anIntegerColumn;
	
	private String anUnannotatedField;
	
	public void setKey(String key) {
		this.key = key;
	}

	public String getKey() {
		return key;
	}

	public String getAStringColumn() {
		return aStringColumn;
	}

	public void setAStringColumn(String aStringColumn) {
		this.aStringColumn = aStringColumn;
	}

	public Integer getAnIntegerColumn() {
		return anIntegerColumn;
	}

	public void setAnIntegerColumn(Integer integerValue) {
		this.anIntegerColumn = integerValue;
	}

	@Override
	public String toString() {
		return "StandardColumnTestClass [key=" + key  + ", aStringColumn=" + aStringColumn
				+ ", anIntegerColumn=" + anIntegerColumn + "]";
	}

	public void setAnUnannotatedField(String anUnannotatedField) {
		this.anUnannotatedField = anUnannotatedField;
	}

	public String getAnUnannotatedField() {
		return anUnannotatedField;
	}

	
}
