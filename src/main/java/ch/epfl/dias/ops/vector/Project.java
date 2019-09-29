package ch.epfl.dias.ops.vector;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.ops.columnar.ColumnarOperator;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

public class Project implements VectorOperator {

	// TODO: Add required structures
	VectorOperator child;
	public int vectorsize;
	int [] columns;
	public Project(VectorOperator child, int[] fieldNo) {
		// TODO: Implement
		this.child = child;
		this.vectorsize = child.getVectorSize();
		this.columns = fieldNo;
	}

	@Override
	public void open() {
		// TODO: Implement
		child.open();
	}

	@Override
	public DBColumn[] next() {
		// TODO: Implement
		DBColumn[] columnsResult = child.next();
		DBColumn[] subsetCols = new DBColumn[columns.length];
		int i=0;
		for(int col:columns) {
			subsetCols[i++] = columnsResult[col];
		}
		return subsetCols;
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
