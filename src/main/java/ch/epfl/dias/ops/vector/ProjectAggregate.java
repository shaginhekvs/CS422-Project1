package ch.epfl.dias.ops.vector;

import java.util.Vector;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.ops.vector.Project;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

public class ProjectAggregate implements VectorOperator {

	// TODO: Add required structures
	VectorOperator child;
	Aggregate agg;
	DataType dt;
	int fieldNum;
	public int vectorsize;
	public ProjectAggregate(VectorOperator child, Aggregate agg, DataType dt, int fieldNo) {
		// TODO: Implement
		int [] fields = {fieldNo};
		this.child = new Project(child, fields) ;
		this.vectorsize = child.getVectorSize();
		this.agg = agg;
		this.dt = dt;
		this.fieldNum=fieldNo;
	}

	@Override
	public void open() {
		// TODO: Implement
		child.open();
	}

	@Override
	public DBColumn[] next() {
		// TODO: Implement
		DBColumn[] columns=null;
		DBColumn[] resultColumns = new DBColumn[1];
		double aggregate=0;
		int count = 0;
		boolean first_time = true;
		do {
			columns = child.next();
			
			if(columns[0].eof)break;
			
			DataType thisType = columns[0].type;

			for (Object current_value: columns[0].fields) {
				double val = 0;
			switch (thisType) {
			case INT:
				val = (double)(int)Integer.valueOf((String)current_value);
				break;
			case BOOLEAN:
				val =  Boolean.valueOf((String)current_value) ? 1.0 : 0.0;
				break;
			case DOUBLE:
				val = Double.valueOf((String)current_value);
				break;
			}
			
			switch (this.agg) {
			case SUM:
				if(first_time) aggregate = 0;
				aggregate+= val;
			case AVG:
				if(first_time) aggregate = 0;
				aggregate+= val;
				count++;
				break;
			case COUNT:
				if(first_time) aggregate = 0;
				aggregate+=1;
				break;
			case MAX:
				if(first_time) aggregate = -Double.MAX_VALUE;
				if(val>aggregate) aggregate = val;
				break;
			case MIN:
				if(first_time) aggregate = Double.MAX_VALUE;
				if(val < aggregate) aggregate = val;
				break;
			default :
				aggregate = 0;
			}
			first_time = false;
			
		}
		}while(true);
		
		if(this.agg==Aggregate.AVG)aggregate = aggregate/(count+Double.MIN_VALUE);
		Object value=null;
		switch (dt) {
		case INT:
			value =  (Integer) (int)aggregate;
			break;
		case DOUBLE:
			value = (Double)aggregate;
			
		}
		value = String.valueOf(value);
		DBColumn thisColumn = new DBColumn(new Vector<Object>(),dt,0);
		thisColumn.fields.add(value);
		resultColumns[0] = thisColumn;
		
		return resultColumns;
	}

	@Override
	public void close() {
		// TODO: Implement
		child.close();
	}

	@Override
	public int getVectorSize() {
		// TODO Auto-generated method stub
		return this.vectorsize;
	}

}
