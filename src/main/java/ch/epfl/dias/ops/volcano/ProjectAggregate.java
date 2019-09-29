package ch.epfl.dias.ops.volcano;

import java.util.ArrayList;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.row.DBTuple;

public class ProjectAggregate implements VolcanoOperator {

	// TODO: Add required structures

	VolcanoOperator scan;
	Aggregate agg;
	DataType dt;
	int fieldNum;
	public ProjectAggregate(VolcanoOperator child, Aggregate agg, DataType dt, int fieldNo) {
		// TODO: Implement
		
		int [] fields = {fieldNo};
		this.scan= new Project(child, fields) ;
		
		this.agg = agg;
		this.dt = dt;
		this.fieldNum=fieldNo;
	}

	@Override
	public void open() {
		// TODO: Implement
		scan.open();
	}

	@Override
	public DBTuple next() {
		// TODO: Implement
		DBTuple tuple = null;
		double aggregate=0;
		int count = 0;
		boolean first_time = true;
		do {
			tuple = scan.next();
			
			if(tuple.eof)break;
			
			double val = 0;
			switch (tuple.types[0]) {
			case INT:
				val = (double)(int)tuple.getFieldAsInt(0);
				break;
			case BOOLEAN:
				val =  tuple.getFieldAsBoolean(0) ? 1.0 : 0.0;
				break;
			case DOUBLE:
				val = tuple.getFieldAsDouble(0);
				break;
			}
			
			switch (this.agg) {
			case SUM:
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
		}while(!tuple.eof);
		if(this.agg == Aggregate.AVG) aggregate /= count+ Double.MIN_VALUE;
		
		Object value=null;
		switch (dt) {
		case INT:
			value =  (Integer) (int)aggregate;
			break;
		case DOUBLE:
			value = (Double)aggregate;
			
		}
		value = String.valueOf(value);
		Object []values = {value};
		DataType[] types = {dt};
		return new DBTuple(values,types);
	}

	@Override
	public void close() {
		// TODO: Implement
		scan.close();
	}

}
