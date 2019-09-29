package ch.epfl.dias.ops.columnar;

import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.column.ColumnStore;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.column.DBColumnIndex;
import ch.epfl.dias.store.row.DBTuple;

public class Scan implements ColumnarOperator {

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
	ColumnStore data;
	boolean lateMaterialization;
	Scan[] scan;
	public Scan(ColumnStore store) {
		// TODO: Implement
		data = store;
		lateMaterialization = store.lateMaterialization;
		this.scan = new Scan[1];
		this.scan[0] = this;
	}

	@Override
	public DBColumn[] execute() {
		// TODO: Implement
		ColumnStore data2 = (ColumnStore) data;
		int rows = data2.getNumColumns();
		int[] array_ = new int[rows];
		for (int i = 0;i<rows;i++ )array_[i] = i;
		
		return data.getColumns(array_);
	}
	
	public DBColumn[] getColumns(int [] array_) {
		// TODO: Implement

		
		return data.getColumns(array_);
	}
	
	

	@Override
	public DBColumn[] lateExecute() {
		// TODO: Implement

		
		DBColumn[] columns = new DBColumn[2];
		DBColumnIndex thisIndex = new DBColumnIndex();
		thisIndex.insertUntil(0, data.getNumRows()-1);
		DBColumnIndex thisIndex2 = new DBColumnIndex();
		thisIndex2.insertUntil(0, data.getNumColumns()-1);
		columns[0] = thisIndex;
		columns[1] = thisIndex2;
		return columns;
	}
	
	public boolean getMaterialization() {
		// TODO: Implement
		return lateMaterialization;
	}
	public Scan [] getScanner() {
		return this.scan;
	}
}
