package org.nate.cassandra;

import org.nate.cassandra.Column;
import org.nate.cassandra.ColumnFamily;
import org.nate.cassandra.Key;

@ColumnFamily(name="Standard1")
public class StandardColumnTestClass {
	
	@Key
	private String key;
	
	@Column
	private String aStringColumn;
	
	@Column
	private Integer anIntegerColumn;
	
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

	
}