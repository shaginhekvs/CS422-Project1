package ch.epfl.dias.ops.columnar;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.column.DBColumnIndex;
import ch.epfl.dias.store.row.DBTuple;

public class Project implements ColumnarOperator {

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
	public boolean lateMaterialization ;
	Scan [] scans;
	int [] columns;
	int columnInAggregate=-1;
	Scan aggScannerToUse;
	public Project(ColumnarOperator child, int[] columns) {
		// TODO: Implement
		this.child = child;
		
		this.lateMaterialization = child.getMaterialization();
		this.scans = child.getScanner();
		this.columns = columns;
		
	}

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
		HashMap<Integer, ArrayList<Integer>> positionToIndices= new HashMap<Integer, ArrayList<Integer>>();
		for (int fieldNo:this.columns) {
			// for each column to be kept, find which table it's in and wht column it corresponds to in that table
		
		for(int i=0,j=0;i<selectedData.length;i+=2,j++) {
			dataIndex = (DBColumnIndex) selectedData[i];
			colIndex = (DBColumnIndex) selectedData[i+1];
			int thisColumnSize = colIndex.indices.size();
			if((columnsPassed+ thisColumnSize)>=fieldNo) {
				tableIndex = i+1;
				scannerToUse = this.scans[j];
				columnIndex = fieldNo-columnsPassed;
				assert(columnIndex >= 0 && columnIndex <= thisColumnSize);
				break;
			}
			columnsPassed += thisColumnSize;
			
		}
		int l=0;
		
		columnIndex = colIndex.indices.get(columnIndex);
		if(columns.length==1) {
			columnInAggregate=columnIndex;
			aggScannerToUse = scannerToUse;
		}
		ArrayList<Integer> indicesToKeep=null;
		if(positionToIndices.containsKey(tableIndex))
			indicesToKeep=positionToIndices.get(tableIndex);
		else{
			indicesToKeep= new ArrayList<Integer>();
			positionToIndices.put(tableIndex, indicesToKeep);
		}
		indicesToKeep.add(columnIndex);

		
		}
		for (Integer position:positionToIndices.keySet()){
			DBColumnIndex thisColumns = (DBColumnIndex)selectedData[position] ;
			thisColumns.indices = positionToIndices.get(position);
		}
		return selectedData;
	}
	
	public boolean getMaterialization() {
		// TODO: Implement
		
		return lateMaterialization;
	}
	public Scan[] getScanner() {
		return this.scans;
	}
}
