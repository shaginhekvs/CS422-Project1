package ch.epfl.dias.ops.columnar;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Vector;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.ops.volcano.VolcanoOperator;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.column.DBColumnIndex;

public class Select implements ColumnarOperator {

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
	
	ColumnarOperator child;
	BinaryOp op;
	int fieldNo;
	String value;
	int index = 0;
	public boolean lateMaterialization ;
	Scan [] scans;
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
	
	public Select(ColumnarOperator child, BinaryOp op, int fieldNo, int value) {
		// TODO: Implement
		this.child = child;
		this.op = op;
		this.fieldNo = fieldNo;
		this.value = String.valueOf(value);
		this.lateMaterialization = child.getMaterialization();
		this.scans = child.getScanner();
	}
	public Select(ColumnarOperator child, BinaryOp op, int fieldNo, Object value) {
		// TODO: Implement
		this.child = child;
		this.op = op;
		this.fieldNo = fieldNo;
		this.value =  String.valueOf(value);
		this.lateMaterialization = child.getMaterialization();
		this.scans = child.getScanner();
	}

	



	@Override
	public DBColumn[] execute() {
		// TODO: Implement
		DBColumn[] selectedData = this.lateExecute(); // get data from child
		
		return EC.commitEager(selectedData,this.scans);
	}
	public DBColumn[] lateExecute() {
		// TODO: Implement
		DBColumn[] selectedData = child.lateExecute();// get indices
		int tableIndex = -1;
		int columnIndex = -1;
		int columnsPassed = 0;
		DBColumnIndex dataIndex=null;
		DBColumnIndex colIndex=null;
		Scan scannerToUse=null;
		
		for(int i=0,j=0;i<selectedData.length;i+=2,j++) {
			dataIndex = (DBColumnIndex) selectedData[i];
			colIndex = (DBColumnIndex) selectedData[i+1];
			int thisColumnSize = colIndex.indices.size();
			if((columnsPassed+ thisColumnSize)>=this.fieldNo) {
				scannerToUse = this.scans[j];
				columnIndex = this.fieldNo-columnsPassed;
				assert(columnIndex >= 0 && columnIndex <= thisColumnSize);
				break;
			}
			columnsPassed += thisColumnSize;
			
		}
		int [] arrayColsIndices = new int[1];
		columnIndex = colIndex.indices.get(columnIndex);
		arrayColsIndices[0] = columnIndex; // use index for the useful column from above
		System.out.println(columnIndex);
		DBColumn[] usefulData = scannerToUse.getColumns(arrayColsIndices); // get data for column of interest for us from the table it is in
		DataType thisColType = usefulData[0].type;
		HashSet<Integer> indicesToDrop = new HashSet<Integer>();
		ArrayList<ArrayList<Integer>> listIndicesToKeep = new ArrayList<ArrayList<Integer>>();
		for(int l=0;l<selectedData.length;l++) {
			listIndicesToKeep.add(new ArrayList<Integer>());
		}
		boolean isFirstTime = true;
		for (int i=0;i<dataIndex.indices.size();i++) {
			Object current_value = usefulData[0].fields.get(dataIndex.indices.get(i));
			boolean result=true;
			switch(thisColType) {
			case INT:
				result = Check.testCondition(Integer.valueOf((String)current_value),Integer.valueOf(this.value), this.op);
				break;
			case DOUBLE:
				result = Check.testCondition(Double.valueOf((String)current_value),Double.valueOf(this.value), this.op);
				break;
			case STRING:
				result = Check.testCondition((String)current_value,this.value, this.op);
				break;
			default:
				result = true;
			}
			if(result) {
				for (int k=0;k<selectedData.length;k+=2) {
					ArrayList<Integer> thisListIndices = listIndicesToKeep.get(k);
					thisListIndices.add(dataIndex.indices.get(i));
				}
				
			}else {
				indicesToDrop.add(i);
			}
		}
		for(int l=0;l<selectedData.length;l+=2) {
			if(listIndicesToKeep.isEmpty())break;
			((DBColumnIndex)selectedData[l]).indices = listIndicesToKeep.get(l);
			
		}
		//dataIndex.indices.removeAll(indicesToDrop);
		
		return selectedData;
	}
	
	public boolean getMaterialization() {
		// TODO: Implement
		return false;
	}
	public Scan[] getScanner() {
		return this.scans;
	}
}
