package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.row.DBTuple;

public class Select implements VolcanoOperator {

	// TODO: Add required structures

	VolcanoOperator scan;
	BinaryOp op;
	int fieldNo;
	String value;
	int index = 0;
	public Select(VolcanoOperator child, BinaryOp op, int fieldNo, int value) {
		// TODO: Implement
		this.scan = child;
		this.op = op;
		this.fieldNo = fieldNo;
		this.value = String.valueOf(value);
		
	}
	
	public Select(VolcanoOperator child, BinaryOp op, int fieldNo, Object value) {
		// TODO: Implement
		this.scan = child;
		this.op = op;
		this.fieldNo = fieldNo;
		this.value =String.valueOf(value);
	}
	

	private static class Check{
		
		static <T extends Comparable<T>> boolean testCondition(T a,T b ,BinaryOp op) {
			switch (op) {
			case EQ:
				//System.out.println(a.equals(b));
				return a.equals(b);
			case LT:
				return a.compareTo(b)<0;
			case LE:
				return a.compareTo(b)<=0;
			case NE:
				return !a.equals(b);
			case GT:
				return a.compareTo(b)>0;
			case GE:
				return a.compareTo(b)>=0;
			default:
				return false;
			}
		}
	}
	@Override
	public void open() {
		// TODO: Implement
		this.scan.open();
	}

	@Override
	public DBTuple next() {
		// TODO: Implement
		DBTuple next=null;
		boolean result=false;
		do {
		 next = scan.next();
		if(next.eof) {
			break;
		}
		result=false;
		switch(next.types[this.fieldNo]) {
		case INT:
			result = Check.testCondition(next.getFieldAsInt(this.fieldNo),Integer.valueOf(this.value), this.op);
			break;
		case DOUBLE:
			result = Check.testCondition((Double)next.getFieldAsDouble(this.fieldNo),Double.valueOf(this.value), this.op);
			break;
		case STRING:
			result = Check.testCondition((String)next.fields[this.fieldNo],this.value, this.op);
			break;
			
		default:
			result = false;
		}
		}while(! result);
		
		return next;
	}

	@Override
	public void close() {
		// TODO: Implement
		this.scan.close();
	}
}
