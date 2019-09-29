package ch.epfl.dias.store.column;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Vector;

import ch.epfl.dias.store.DataType;

public class DBColumn {
	public Vector<Object> fields;
	public DataType type;
	public boolean eof;
	public int columnID;
	public DBColumn(Vector<Object> fields, DataType type,int colID) {
		this.fields = fields;
		this.type = type;
		this.eof = false;
		this.columnID = colID;
	}

	public DBColumn() {
		this.eof = true;
	}
	
	/**
	 * XXX Assuming that the caller has ALREADY checked the datatype, and has
	 * made the right call
	 * 
	 * @param fieldNo
	 *            (starting from 0)
	 * @return cast of field
	 */
	public Integer[] getAsInteger() {
		Integer [] second_data = new Integer[fields.size()];
		int i=0;
		for (Object o:fields) {
			System.out.println(o instanceof String);
			second_data[i++]=Integer.valueOf((String)o);
		}
		return  second_data;
	}

	public Double[] getAsDouble() {
		Double [] second_data = new Double[fields.size()];
		int i=0;
		for (Object o:fields) {
			second_data[i++] = Double.valueOf((String)o);
		}
		return second_data;
	}
	
	public Boolean[] getAsBoolean() {
		Boolean[] second_data = new Boolean[fields.size()];
		int i=0;
		for (Object o:fields) {
			second_data[i++] =  Boolean.valueOf((String)o);
		}
		return second_data;
	}
	
	public String[] getAsString() {
		String[] second_data = new String[fields.size()];
		int i=0;
		for (Object o:fields) {
			second_data[i++] = (String)o;
		}
		return second_data;
	}



	@Override
	public String toString() {
		StringBuilder data = new StringBuilder();
		for (int i=0;i<this.fields.size();i++) {
			data.append(fields.get(i)+",");
		}
		return data.toString();
	}
	

	
}
