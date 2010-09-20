package org.nate.cassandra;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.nate.functions.options.Option;

import com.google.common.collect.Lists;

public class ResultSet<T> { 

	public List<T> results = new ArrayList<T>();
	private CassandraOperationUtils opUtils = new CassandraOperationUtils();
	
	public ResultSet<T> or(ResultSet<T> other) throws IllegalArgumentException, IllegalAccessException {
		ResultSet<T> sumSet = new ResultSet<T>();
		sumSet.results.addAll(this.results);
		for (T otherResult : other.results) {			
			if (!matchedKeyValues(otherResult)) {
				sumSet.results.add(otherResult);
			}
		}
		return sumSet;
	}
	
	public ResultSet<T> and(ResultSet<T> other) throws IllegalArgumentException, IllegalAccessException {
		ResultSet<T> sumSet = new ResultSet<T>();
		for (T otherResult : other.results) {
			if (matchedKeyValues(otherResult)) {
				sumSet.results.add(otherResult);
			}
		}
		return sumSet;
	}
	
	public ResultSet<T> ascendingBy(String fieldName) throws CassandraOperationException {
		ResultSet<T> sortedResultSet = new ResultSet<T>();
		sortedResultSet.results.addAll(this.results);
		
		try {
			if (!this.results.isEmpty()) {
				Field sortOnField = sortedResultSet.results.get(0).getClass().getDeclaredField(fieldName);
				Collections.sort(sortedResultSet.results, new AscendingSorter(sortOnField));
			}
		} catch (Exception e) {
			throw new CassandraOperationException("Unable to sort result set ascending", e);
		}
		return sortedResultSet;
	}
	
	public ResultSet<T> descendingBy(String fieldName) throws CassandraOperationException {
		ResultSet<T> sortedResultSet = new ResultSet<T>();
		sortedResultSet.results.addAll(this.results);
		
		try {
			if (!this.results.isEmpty()) {
				Field sortOnField = sortedResultSet.results.get(0).getClass().getDeclaredField(fieldName);
				Collections.sort(sortedResultSet.results, new DescendingSorter(sortOnField));
			}
		} catch (Exception e) {
			throw new CassandraOperationException("Unable to sort result set descending", e);
		}
		return sortedResultSet;
	}
	
	private boolean matchedKeyValues(T otherResult) throws IllegalArgumentException, IllegalAccessException {
		boolean found = false;
		Option<Field> otherKeyOption = opUtils.keyFieldFor(Lists.newArrayList(otherResult.getClass().getDeclaredFields()));
		if (otherKeyOption.isSome()) {
			for (T thisResult : this.results) {
				Option<Field> thisKeyOption = opUtils.keyFieldFor(Lists.newArrayList(thisResult.getClass().getDeclaredFields()));
				if (thisKeyOption.isSome()) {
					otherKeyOption.get().setAccessible(true);
					thisKeyOption.get().setAccessible(true);
					if (thisKeyOption.get().get(thisResult).equals(otherKeyOption.get().get(otherResult))) {
						found = true;
					}
				}
			}
		} else {
			throw new IllegalArgumentException("The objects you are trying to perform operations on do not have keys");
		}
		return found;
	}
	
	private abstract class Sorter implements Comparator<T> {
		protected Field field;

		public Sorter(Field field) {
			this.field = field;
		}
		
		abstract public int compare(T o1, T o2);	
	}
	
	private class AscendingSorter extends Sorter {
		
		public AscendingSorter(Field field) {
			super(field);
		}

		@Override
		public int compare(T o1, T o2) {
			try {
				field.setAccessible(true);
				Object fieldValue1 = field.get(o1);
				Object fieldValue2 = field.get(o2);
				
				if (fieldValue1 instanceof String) {
					return ((String) fieldValue1).compareToIgnoreCase((String) fieldValue2);
				} else if (fieldValue1 instanceof Number){
					return ((Number) fieldValue1).intValue() - ((Number) fieldValue2).intValue();
				} else {
					return 0;
				}
			} catch (IllegalArgumentException e) {
				return 0;
			} catch (IllegalAccessException e) {
				return 0;
			}
		}	
	}
	
	private class DescendingSorter extends Sorter {
		
		public DescendingSorter(Field field) {
			super(field);
		}

		@Override
		public int compare(T o1, T o2) {
			try {
				field.setAccessible(true);
				Object fieldValue1 = field.get(o1);
				Object fieldValue2 = field.get(o2);
				
				if (fieldValue1 instanceof String) {
					return ((String) fieldValue2).compareToIgnoreCase((String) fieldValue1);
				} else if (fieldValue1 instanceof Number){
					return ((Number) fieldValue2).intValue() - ((Number) fieldValue1).intValue();
				} else {
					return 0;
				}
			} catch (IllegalArgumentException e) {
				return 0;
			} catch (IllegalAccessException e) {
				return 0;
			}
		}	
	}
}
