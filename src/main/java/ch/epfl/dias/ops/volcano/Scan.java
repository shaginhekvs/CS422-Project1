package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.row.DBTuple;

public class Scan implements VolcanoOperator {

	// TODO: Add required structures
	int index;
	boolean end_scan = false;
	Store data;
	public Scan(Store store) {
		// TODO: Implement
		data = store;

	}

	@Override
	public void open() {
		// TODO: Implement
		index = 0;
	}

	@Override
	public DBTuple next() {
		// TODO: Implement
		DBTuple next_tuple = data.getRow(this.index++);
		return next_tuple;
	}

	@Override
	public void close() {
		// TODO: Implement
		index = 0;
	}
}