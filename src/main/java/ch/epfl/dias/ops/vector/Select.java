package ch.epfl.dias.ops.vector;

import java.util.Vector;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.ColumnStore;
import ch.epfl.dias.store.column.DBColumn;

public class Select implements VectorOperator {

	// TODO: Add required structures

	public int vectorsize;
	int lastDataPointer;
	DBColumn[] lastGottenData;
	VectorOperator child;
	BinaryOp op;
	int fieldNo;
	String value;
	
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
	
	public Select(VectorOperator child, BinaryOp op, int fieldNo, int value) {
		// TODO: Implement
		this.child = child;
		this.op = op;
		this.fieldNo = fieldNo;
		this.value = String.valueOf(value);
		this.vectorsize = child.getVectorSize();
		lastDataPointer = -1;
		lastGottenData = null;
	}
	
	
	public Select(VectorOperator child, BinaryOp op, int fieldNo, Object value) {
		// TODO: Implement
		this.child = child;
		this.op = op;
		this.fieldNo = fieldNo;
		this.value =String.valueOf(value);
	}
	
	@Override
	public void open() {
		// TODO: Implement
		child.open();
	}

	private boolean selectThis(Object currentValue,DataType thisColType) {
		boolean result=true;
		switch(thisColType) {
		case INT:
			result = Check.testCondition(Integer.valueOf((String)currentValue),Integer.valueOf(value), this.op);
			break;
		case DOUBLE:
			result = Check.testCondition(Double.valueOf((String)currentValue),Double.valueOf(this.value), this.op);
			break;
		case STRING:
			result = Check.testCondition((String)currentValue,this.value, this.op);
			break;
		default:
			result = true;
		}
		return result;
	}

	@Override
	public DBColumn[] next(){
		// TODO: Implement
		int currentTuples = 0;
		DBColumn[] resultColumns =null;
		int i;
		int j;
		boolean isFirst = true;
		do {
			DBColumn[] columns = child.next();
			
			if(isFirst) {
				resultColumns = new DBColumn[columns.length];
				if(this.lastDataPointer>=0) {
					// some data remaining from previous next, use that first
					for ( i=0;i<resultColumns.length;i++) {
						resultColumns[i] = new DBColumn(new Vector<Object>(),columns[i].type,i);
					}
					for ( j = lastDataPointer;j< lastGottenData[0].fields.size();j++) {
						if (selectThis(lastGottenData[this.fieldNo].fields.get(j), lastGottenData[this.fieldNo].type)) {
							currentTuples++;
							for ( i=0;i<resultColumns.length;i++) {
								resultColumns[i].fields.add(lastGottenData[i].fields.get(j));
							}	
						}
					}
				isFirst = false; // don't recreate DBTuple instances
				this.lastDataPointer=-1; // previous data used , so enable that
				}
			}
			if(columns[0].eof){
				if(isFirst) {
					for ( i=0;i<resultColumns.length;i++) {
						resultColumns[i] = new DBColumn();
					}
				isFirst = false;
				}
				break;
			}
			
			if(isFirst) { 
				for ( i=0;i<resultColumns.length;i++) {
					resultColumns[i] = new DBColumn(new Vector<Object>(),columns[i].type,i);
				}
				isFirst = false;
			}
			
			for (j = 0; j<columns[0].fields.size();j++) {
				if(currentTuples>=vectorsize) {
					// means current vector is full , so save remaining tuples for next iteration
					lastDataPointer = j;
					lastGottenData = columns;
					break;	
				}
				// else add jth tuple to result if selected 
				if (selectThis(columns[this.fieldNo].fields.get(j), columns[this.fieldNo].type)) {
					currentTuples++;
					//System.out.println(currentTuples);
					for (i=0;i<resultColumns.length;i++) {
						resultColumns[i].fields.add(columns[i].fields.get(j));
					}
				}
				
			}
		}while(currentTuples<this.vectorsize);
		return resultColumns;
	}

	
	@Override
	public int getVectorSize() {
		// TODO Auto-generated method stub
		return this.vectorsize;
	}
	@Override
	public void close() {
		// TODO: Implement
		child.close();
	}
}
