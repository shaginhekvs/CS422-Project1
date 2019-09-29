package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.row.DBTuple;
import java.util.Arrays;

import javax.xml.crypto.Data;

public class Project implements VolcanoOperator {

	// TODO: Add required structures

	VolcanoOperator scan;
	int [] fieldNo;
	int maxField ;
	public Project(VolcanoOperator child, int[] fieldNo) {
		// TODO: Implement
		scan = child;
		this.fieldNo= fieldNo;
		maxField = Arrays.stream(fieldNo).max().getAsInt();
	}

	@Override
	public void open() {
		// TODO: Implement
		scan.open();
	}

	@Override
	public DBTuple next() {
		// TODO: Implement
		//System.out.println(fieldNo.length);
		DBTuple tuple = scan.next();
		
		if(tuple.eof ) return tuple;
		
		//if (maxField>= tuple.fields.length) return new DBTuple();
		
		
		Object[] fields = new Object[fieldNo.length];
		DataType [] types = new DataType[fieldNo.length];
		int j=0;
		for (int field:fieldNo) {
			fields[j] = tuple.fields[field];
			types[j] = tuple.types[field];
			j++;
		}
		
		return new DBTuple(fields,types) ;
	}

	@Override
	public void close() {
		// TODO: Implement
		scan.close();
	}
}
