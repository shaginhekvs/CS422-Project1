package ch.epfl.dias.ops.vector;


import java.util.Vector;

import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.column.ColumnStore;
import ch.epfl.dias.store.column.DBColumn;

public class Scan implements VectorOperator {

	// TODO: Add required structures

	ColumnStore store;
	public int vectorsize;
	int currentPointer;
	int MAXSIZE = 0;
	DBColumn[] data;
	public Scan(Store store, int vectorsize) {
		// TODO: Implement
		this.store = (ColumnStore)store;
		this.vectorsize = vectorsize;
		this.currentPointer = 0;
		int cols = this.store.getNumColumns();
		int[] array_ = new int[cols];
		for (int i = 0;i<cols;i++ )array_[i] = i;
		this.data = store.getColumns(array_);
		MAXSIZE = data[0].fields.size();
	}
	
	@Override
	public void open() {
		// TODO: Implement
		currentPointer = 0;
		
	}

	@Override
	public DBColumn[] next() {
		// TODO: Implement
		DBColumn[] columns = new DBColumn[data.length];
		boolean isDone  = false;
		int i = 0;
		for ( i=0;i<columns.length;i++)
		{
			if(currentPointer>=MAXSIZE) {
				isDone = true;
				columns[i] = new DBColumn();
			}
			else {
				columns[i] = new DBColumn(new Vector<Object>(),data[i].type,i);
			}

		}
		if(isDone)
			return columns;
		
		for ( i=0;i<columns.length;i++)
		{
			for (int j=currentPointer;j<currentPointer+vectorsize;j++) {
				if(j>=MAXSIZE)break;
				columns[i].fields.add(data[i].fields.get(j));
			}
		}
		currentPointer += vectorsize;
		return columns;
	}
	
	@Override
	public int getVectorSize() {
		// TODO Auto-generated method stub
		return this.vectorsize;
	}

	@Override
	public void close() {
		// TODO: Implement
		currentPointer = 0;
	}
}
