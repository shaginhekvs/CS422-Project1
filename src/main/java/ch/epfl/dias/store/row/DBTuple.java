package ch.epfl.dias.store.row;

import java.util.Arrays;

import ch.epfl.dias.store.DataType;

public class DBTuple {
	public Object[] fields;
	public DataType[] types;
	public boolean eof;

	public DBTuple(Object[] fields, DataType[] types) {
		this.fields = fields;
		this.types = types;
		this.eof = false;
	}

	public DBTuple() {
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
	public Integer getFieldAsInt(int fieldNo) {
		return Integer.valueOf((String)fields[fieldNo]);
	}

	public Double getFieldAsDouble(int fieldNo) {
		return Double.valueOf((String)fields[fieldNo]);
	}

	public Boolean getFieldAsBoolean(int fieldNo) {
		return Boolean.valueOf((String)fields[fieldNo]);
	}

	public String getFieldAsString(int fieldNo) {
		return (String) fields[fieldNo];
	}

	public static DBTuple join(DBTuple left , DBTuple right) {
		Object[] fields = new Object[left.fields.length+right.fields.length];
		DataType [] types = new DataType[left.fields.length+right.fields.length];
		int i=0;
		int j=0;
		for (i= 0,j=0;j<left.fields.length;i++,j++) {
			fields[i]=left.fields[j];
			types[i]=left.types[j];
		}
		for(j=0;j<right.fields.length;j++,i++) {
			fields[i]=right.fields[j];
			types[i]=right.types[j];
		}
		return new DBTuple(fields,types);
		
	}
	@Override
	public String toString() {
		StringBuilder data = new StringBuilder();
		for (int i=0;i<this.fields.length;i++) {
			data.append(fields[i]+",");
			
		}
		return data.toString();
	}
	
	
}
