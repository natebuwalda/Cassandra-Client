package org.nate.cassandra;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.commons.lang.StringUtils;
import org.nate.cassandra.annotation.ColumnFamily;
import org.nate.cassandra.annotation.Key;
import org.nate.functions.functors.FilterFn;
import org.nate.functions.functors.ListFunctions;
import org.nate.functions.options.Option;

public class CassandraOperationUtils {

	public Column createColumnObject(Field field, Object fieldValue) {
		Column column = new Column();
		column.setName(field.getName().getBytes());
		column.setValue(convertValueToString(fieldValue).getBytes());
		column.setTimestamp(System.currentTimeMillis());
		return column;
	}


	public ColumnPath createColumnPath(final String columnFamilyName, byte[] columnName) {
		ColumnPath columnPath = new ColumnPath();
		columnPath.setColumn(columnName);
		columnPath.setColumn_family(columnFamilyName);
		return columnPath;
	}


	public SlicePredicate createEmptySlicePredicate() {
		SlicePredicate slicePredicate = new SlicePredicate();
		SliceRange sliceRange = new SliceRange();
		sliceRange.setStart(new byte[] {});
		sliceRange.setFinish(new byte[] {});
		slicePredicate.setSlice_range(sliceRange);
		return slicePredicate;
	}

	public String determineColumnFamily(Class<? extends Object> clazz) throws IllegalAccessException,
			InvocationTargetException {
		String columnFamilyName = null;
		try {
			Annotation runtimeAnnotation = clazz.getAnnotation(ColumnFamily.class);
			if (runtimeAnnotation != null) {
				Method nameMethod = ((Class<? extends Annotation>) runtimeAnnotation.getClass()).getMethod("name", new Class<?>[]{});
				columnFamilyName  = (String) nameMethod.invoke(runtimeAnnotation, new Object[]{});
			}
		} catch (NoSuchMethodException nsme) {
			//eat this exception because we do not care
		}
		
		if (columnFamilyName  == null || columnFamilyName.isEmpty()) {
			columnFamilyName  = clazz.getSimpleName();
		}
		return columnFamilyName;
	}
	
	public String determineColumnName(Field field) throws IllegalAccessException, InvocationTargetException {
		String columnName = null;
		
		try {
			Annotation runtimeAnnotation = field.getAnnotation(org.nate.cassandra.annotation.Column.class);
			if (runtimeAnnotation != null) {
				Method nameMethod = ((Class<? extends Annotation>) runtimeAnnotation.getClass()).getMethod("name", new Class<?>[]{});
				columnName = (String) nameMethod.invoke(runtimeAnnotation, new Object[]{});
			}
		} catch (NoSuchMethodException nsme) {
			//eat this exception because we do not care
		}
		
		if (columnName == null || columnName.isEmpty()) {
			columnName = field.getName();
		} 
		return columnName;
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" }) 
	public Object convertStringToValue(String string, Class fieldType) throws IllegalArgumentException, SecurityException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		Object converted = null;
		
		if ("String".equals(fieldType.getSimpleName())) {
			converted = string;
		} else if ("byte[]".equals(fieldType.getSimpleName())) {
			converted = string.getBytes();
		} else if (StringUtils.isNumeric(string)) {
			converted = fieldType.getConstructor(new Class[]{String.class}).newInstance(new Object[]{string});
		}
		
		return converted;
	}


	public String convertValueToString(Object value) {
		String converted = null;
		if (value instanceof String) {
			converted = (String) value;
		} else if (value instanceof byte[]) {
			converted = new String((byte[]) value);
		} else if (value instanceof Number){
			converted = ((Number) value).toString();
		} else {
			throw new IllegalArgumentException("Value must be a String or a Number, was " + value.toString());
		}
		return converted;
	}

	
	public Option<Field> keyFieldFor(List<Field> declaredFields) {
		Option<Field> keyFieldOption = ListFunctions.find(declaredFields, new FilterFn<Field>(){
			public boolean apply(Field it) {
				return it.isAnnotationPresent(Key.class);
			}	
		});
		return keyFieldOption;
	}
}
