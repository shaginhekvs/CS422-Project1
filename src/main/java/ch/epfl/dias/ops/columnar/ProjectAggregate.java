package ch.epfl.dias.ops.columnar;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Vector;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.columnar.Project;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.column.DBColumnIndex;

public class ProjectAggregate implements ColumnarOperator {

	   /*
	    * The late materialization is implemented so that all the necessary columns are materialized 
	    * only when aggregate is called or at the last operator before the data is emitted to the user. 
	    * This was accomplished by passing the ids of row indices and column indices as output of 
	    * every operator. Select and projection led to shrinking this Set of indices. 
	    * In case of Hash Join , additional lists were appended corresponding to every table. \
	    * The scan operator was also necessary to pass down the pipeline in this architecture 
	    * so that every operator can fetch just the column it needs. 
	    * To account for joins, multiple scans were sent . 
	    * 
	    */
	Project projectChild;
	public boolean lateMaterialization ;
	Scan[] scans;
	Aggregate agg;
	DataType dt;
	int fieldNum;
	ColumnarOperator child;
	public ProjectAggregate(ColumnarOperator child, Aggregate agg, DataType dt, int fieldNo) {
		// TODO: Implement
		int [] fields = {fieldNo};
		this.projectChild = new Project(child, fields);
		this.child = child;
		this.lateMaterialization = child.getMaterialization();
		this.scans = child.getScanner();
		
		this.agg = agg;
		this.dt = dt;
		this.fieldNum=fieldNo;
		
	}

	@Override
	public DBColumn[] execute() {
		// TODO: Implement
		return this.lateExecute();
	}
	public DBColumn[] lateExecute() {
		// TODO: Implement
		int columnIndex = -1;
		double aggregate=0;
		
		DBColumn[] selectedData = projectChild.lateExecute();
		if(projectChild.columnInAggregate>=0)
			columnIndex = projectChild.columnInAggregate;
		else assert false;
		Scan scannerToUse = projectChild.aggScannerToUse;
		DBColumnIndex dataIndex = (DBColumnIndex) selectedData[0];
		
		DBColumnIndex indices =dataIndex;
		int []array_=new int[1];
		array_[0] = columnIndex;
		DBColumn[] data = scannerToUse.getColumns(array_);
		DataType thisType = data[0].type;
		boolean first_time = true;
		int count = 0;
		for (int j: indices.indices) {
			
			Object current_value = data[0].fields.get(j) ;
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
		case AVG:
			if(first_time) aggregate = 0;
			aggregate+= val;
			count++;
			break;
		case COUNT:
			if(first_time) aggregate = 0;
			aggregate+=1;
			count++;
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
		
		Object value=null;
		switch (dt) {
		case INT:
			value =  (Integer) (int)aggregate;
			break;
		case DOUBLE:
			value = (Double)aggregate;
			
		}
		if(this.agg == Aggregate.AVG) aggregate /= count+ Double.MIN_VALUE;
		value = String.valueOf(value);
		DBColumn [] columns = new DBColumn[1];
		DBColumn thisColumn = new DBColumn(new Vector<Object>(),dt,0);
		thisColumn.fields.add(value);
		columns[0] = thisColumn;
		return columns;
	}
	
	public boolean getMaterialization() {
		// TODO: Implement
		return false;
	}
	public Scan[] getScanner() {
		return null;
	}
}
