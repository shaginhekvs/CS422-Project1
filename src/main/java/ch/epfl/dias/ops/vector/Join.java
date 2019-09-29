package ch.epfl.dias.ops.vector;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.ops.columnar.ColumnarOperator;
import ch.epfl.dias.ops.vector.EC;
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

public class Join implements VectorOperator {

	// TODO: Add required structures
	
	VectorOperator leftChild;
	VectorOperator rightChild;
	DBColumn[] leftData;
	DBColumn[] rightData;
	HashMap<Key, List> map;
	int leftFieldNo;
	int rightFieldNo;
	ArrayList<DBColumn> columns;
	int currentPointer;
	public int vectorsize;
	DBColumn[] data;
	int MAXSIZE = 0;
	public Join(VectorOperator leftChild, VectorOperator rightChild, int leftFieldNo, int rightFieldNo) {
		// TODO: Implement
		this.leftChild = leftChild;
		this.currentPointer = 0;
		this.vectorsize = leftChild.getVectorSize();
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

	
	private void innerJoin(DBColumn [] leftColumns , DBColumn [] rightColumns) {

		DBColumnIndex leftIndex = (DBColumnIndex) leftColumns[0];
		DBColumnIndex rightIndex = (DBColumnIndex) rightColumns[0];
		int realLeftFieldIndex = leftFieldNo+1;
		int realRightFieldIndex = rightFieldNo+1;
		
		
		for (int j:leftIndex.indices) {
			Key<String> thisKey = new Key<String>((String)leftColumns[realLeftFieldIndex].fields.get(j),leftColumns[realLeftFieldIndex].type);
			addToMap(thisKey, j, true);
		}
		
		
		
		for (int i:rightIndex.indices) {
			Key<String> thisKey = new Key<String>((String)rightColumns[realRightFieldIndex].fields.get(i),rightColumns[realRightFieldIndex].type);
			addToMap(thisKey, i, false);
		}	
		
		int count = 0;
		DBColumnIndex thisIndex =(DBColumnIndex) columns.get(0);
		for (HashMap.Entry<Key, List> entry:map.entrySet()) {
			List<List<Integer>> outer_list = entry.getValue();
			if(outer_list.size()==2) {
				//both left and right sides present
				for(int leftTuple:outer_list.get(0)) {
					for (int rightTuple:outer_list.get(1)) {
						thisIndex.indices.add(count++);
						int i;
						for (i = 1;i<leftColumns.length;i++){
							columns.get(i).fields.add(leftColumns[i].fields.get(leftTuple));
						}
						for (int k = 1;k<rightColumns.length;k++,i++) {
							columns.get(i).fields.add(rightColumns[k].fields.get(leftTuple));
						}
						
					}
				}
			}
		}
	}
	
	private DBColumn[] insertIndex(DBColumn[] allCols) {
		int numRows = allCols[0].fields.size(); // number of rows present
		
		DBColumn[] columns = new DBColumn[allCols.length+1];
		
		DBColumnIndex thisIndex = new DBColumnIndex();
		thisIndex.insertUntil(0, numRows-1);
		columns[0] = thisIndex;
		
		for(int i=0;i<allCols.length;i++) {
			columns[i+1] = allCols[i];
		}
		allCols = columns;
		return columns;
	}
	private DBColumn[] makeFullData(VectorOperator child, DBColumn [] localData) {
		DBColumn [] curResult=null;
		boolean isFirst = true;
		do {
			curResult = child.next();
			if(isFirst) {
				localData = new DBColumn[curResult.length];
				for (int i=0;i<curResult.length;i++) {
					localData[i] = curResult[i];
				}
				isFirst = false;
				continue;
			}
			if(curResult[0].eof)break;
			for (int i=0;i<curResult.length;i++) {
				for(Object o:curResult[i].fields) {
					localData[i].fields.add(o);
				}
			}
						
		}while(!curResult[0].eof);
		return localData;
	}

	@Override
	public void open() {
		// TODO: Implement
		leftChild.open();
		rightChild.open();
		leftData = makeFullData(leftChild, leftData);
		rightData = makeFullData(rightChild, rightData);
		leftData = insertIndex(leftData);
		rightData = insertIndex(rightData);
		DBColumnIndex newIndex = new DBColumnIndex();
		columns.add(newIndex);
		for (int k = 1;k<(leftData.length);k++)
		{
			columns.add(new DBColumn(new Vector<Object>(),leftData[k].type,k));
		}
		for (int j = 1;j<rightData.length;j++) {
			columns.add(new DBColumn(new Vector<Object>(),rightData[j].type,j));
		}
		innerJoin(leftData,rightData);
		this.currentPointer = 0;
		this.data = columns.toArray(new DBColumn[columns.size()]);
		this.data = EC.commitEager(data);
		MAXSIZE = data[0].fields.size();
		
		
		System.out.println("done map");
		
	}

	@Override
	public DBColumn[] next() {
		// TODO: Implement
		DBColumn[] columns = new DBColumn[data.length-1];
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
	public void close() {
		// TODO: Implement
		leftChild.close();
		rightChild.close();
	}

	@Override
	public int getVectorSize() {
		// TODO Auto-generated method stub
		return this.vectorsize;
	}
	
}
