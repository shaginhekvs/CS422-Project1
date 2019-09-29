package ch.epfl.dias.ops.columnar;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.ops.volcano.Key;
import ch.epfl.dias.ops.volcano.VolcanoOperator;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.column.DBColumnIndex;
import ch.epfl.dias.store.row.DBTuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.stream.Stream;

public class Join implements ColumnarOperator {

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
	HashMap<Key, List> map;
	ColumnarOperator leftChild;
	ColumnarOperator rightChild;
	int leftFieldNo;
	int rightFieldNo;
	ArrayList<DBColumn> columns;
	Scan [] leftScans;
	Scan [] rightScans;
	int index = 0;

	public Join(ColumnarOperator leftChild, ColumnarOperator rightChild, int leftFieldNo, int rightFieldNo) {
		// TODO: Implement
		this.leftChild = leftChild;
		this.leftScans = leftChild.getScanner();
		this.rightScans = rightChild.getScanner();
		this.rightChild = rightChild;
		this.leftFieldNo = leftFieldNo;
		this.rightFieldNo = rightFieldNo;
		map = new HashMap<Key,List>();
		columns = new  ArrayList<DBColumn>();
		
	}

	private void addToMap(Key thisKey,int tuple,boolean probeLeft) {
		
		if(!map.containsKey(thisKey)) {
			if(probeLeft) {
			ArrayList<List> outer_list = new ArrayList<List>();
			ArrayList<Integer> this_list = new ArrayList<Integer>();
			this_list.add(tuple);
			outer_list.add(this_list);
			map.put(thisKey, outer_list);
			}
		}
		else {
			if(probeLeft) {
			List<List> outer_list = map.get(thisKey);
			List<Integer> inner_list = outer_list.get(0);
			inner_list.add(tuple);
			}
			else {
				List<List> outer_list = map.get(thisKey);
				int sizeOuterList = outer_list.size();
				if(sizeOuterList==0) {
					System.out.println("unexpected, no value, but key present");
				}
				else if (sizeOuterList==1) {
					ArrayList<Integer> this_list = new ArrayList<Integer>();
					this_list.add(tuple);
					outer_list.add(this_list);
				}
				else if(sizeOuterList==2) {
					List<List> outer_list1 = map.get(thisKey);
					List<Integer> inner_list1 = outer_list1.get(1);
					inner_list1.add(tuple);
					
				}
				else
					System.out.println("unexpected length of outer list can't be more than 2");
				
			}
		}
	}

	private Object[] getIndexOfRelevantTable(DBColumn[] selectedData,Scan[] scans,int relevantFieldNum) {
		int tableIndex = -1;
		int columnIndex = -1;
		int columnsPassed = 0;
		DBColumnIndex dataIndex=null;
		DBColumnIndex colIndex=null;
		Scan scannerToUse=null;
		Object[] arrayResult = new Object[4];
		for(int i=0,j=0;i<selectedData.length;i+=2,j++) {
			dataIndex = (DBColumnIndex) selectedData[i];
			colIndex = (DBColumnIndex) selectedData[i+1];
			int thisColumnSize = colIndex.indices.size();
			if((columnsPassed+ thisColumnSize)>=relevantFieldNum) {
				scannerToUse = scans[j];
				columnIndex =relevantFieldNum-columnsPassed;
				assert(columnIndex >= 0 && columnIndex <= thisColumnSize);
				break;
			}
			columnsPassed += thisColumnSize;
			
		}
		int [] arrayColsIndices = new int[1];
		columnIndex = colIndex.indices.get(columnIndex);
		arrayResult[0]= dataIndex;
		arrayResult[1]=colIndex;
		arrayResult[2]=scannerToUse;
		arrayResult[3]=new Integer(columnIndex);

	return 	arrayResult;
	}
	private void innerJoin(Object[] leftColumns , Object [] rightColumns) {

		DBColumnIndex leftIndex = (DBColumnIndex) leftColumns[0];
		DBColumnIndex rightIndex = (DBColumnIndex) rightColumns[0];
		int [] array_ = new int[1];
		array_[0]= (int)leftColumns[3];
		DBColumn [] leftUsefulColumn = ((Scan)leftColumns[2]).getColumns(array_);
		array_[0] = (int)rightColumns[3];
		DBColumn [] rightUsefulColumn = ((Scan)rightColumns[2]).getColumns(array_);
		int realLeftFieldIndex = 0;
		int realRightFieldIndex = 0;
		
		
		for (int j=0;j<leftIndex.indices.size();j++) {
			Key<String> thisKey = new Key<String>((String)leftUsefulColumn[realLeftFieldIndex].fields.get(leftIndex.indices.get(j)),leftUsefulColumn[realLeftFieldIndex].type);
			addToMap(thisKey, j, true);
		}
		
		
		
		for (int i=0;i<rightIndex.indices.size();i++) {
			Key<String> thisKey = new Key<String>((String)rightUsefulColumn[realRightFieldIndex].fields.get(rightIndex.indices.get(i)),rightUsefulColumn[realRightFieldIndex].type);
			addToMap(thisKey, i, false);
		}	
		
		int count = 0;
		DBColumnIndex thisIndex1 =(DBColumnIndex) columns.get(0);
		DBColumnIndex thisIndex2 =(DBColumnIndex) columns.get(1);
		for (HashMap.Entry<Key, List> entry:map.entrySet()) {
			List<List<Integer>> outer_list = entry.getValue();
			if(outer_list.size()==2) {
				//both left and right sides present
				for(int leftTuple:outer_list.get(0)) {
					for (int rightTuple:outer_list.get(1)) {
						thisIndex1.indices.add(leftTuple);
						thisIndex2.indices.add(rightTuple);
					}
				}
			}
		}
	}
	
	
	public DBColumn[] execute() {
		// TODO: Implement
		return EC.commitEager(this.lateExecute(),this.getScanner());
	}
	
	public DBColumn[] lateExecute() {
		// TODO: Implement

		DBColumn[] leftColumns = leftChild.lateExecute();
		DBColumn[] rightColumns = rightChild.lateExecute();
		columns.add(new DBColumnIndex());
		columns.add(new DBColumnIndex());
		Object [] leftTableThings = getIndexOfRelevantTable(leftColumns, leftScans, this.leftFieldNo);
		Object [] rightTableThings = getIndexOfRelevantTable(rightColumns, rightScans, this.rightFieldNo);
		DBColumnIndex leftIndex = (DBColumnIndex) leftTableThings[0];
		DBColumnIndex rightIndex = (DBColumnIndex) rightTableThings[0];
		
		innerJoin(leftTableThings,rightTableThings);
		
		DBColumn[] indicesAfterJoin = new DBColumn [leftColumns.length+rightColumns.length];
		boolean isFirst = true;
		for (int i:((DBColumnIndex)columns.get(0)).indices) {
			for (int j=0;j<leftColumns.length;j+=2) {
				if(isFirst) {
					indicesAfterJoin[j] = new DBColumnIndex();
					indicesAfterJoin[j+1] = leftColumns[j+1];
				}
				((DBColumnIndex)indicesAfterJoin[j]).indices.add(((DBColumnIndex)leftColumns[j]).indices.get(i)); 
			
			}
			isFirst=false;
		}
		isFirst=true;
		for (int i:((DBColumnIndex)columns.get(1)).indices) {
			for(int l = leftColumns.length,m = 0;m<rightColumns.length;m+=2,l+=2) {
				if(isFirst) {
					indicesAfterJoin[l] = new DBColumnIndex();
					indicesAfterJoin[l+1] = rightColumns[m+1];
				}
				((DBColumnIndex)indicesAfterJoin[l]).indices.add(((DBColumnIndex)rightColumns[m]).indices.get(i)); 
				
			}
			isFirst=false;
		}
		
		
		
		return indicesAfterJoin;
	}
	
	public boolean getMaterialization() {
		// TODO: Implement
		return false;
	}
	
	public Scan[] getScanner() {
		Scan [] combined = new Scan[leftScans.length+rightScans.length];
		int i=0,j=0;
		for (i=0;i<leftScans.length;i++)
			combined[i] = leftScans[i];
		for(j=0;j<rightScans.length;j++,i++)
			combined[i] = rightScans[j];
		return combined;
	}
}
