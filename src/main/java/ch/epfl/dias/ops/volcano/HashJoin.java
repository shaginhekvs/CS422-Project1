package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.row.DBTuple;
import ch.epfl.dias.ops.volcano.Key;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class HashJoin implements VolcanoOperator {

	// TODO: Add required structures
	

	
	HashMap<Key, List> map;
	VolcanoOperator leftChild;
	VolcanoOperator rightChild;
	int leftFieldNo;
	int rightFieldNo;
	ArrayList<DBTuple> all_tuples;
	int index = 0;
	public HashJoin(VolcanoOperator leftChild, VolcanoOperator rightChild, int leftFieldNo, int rightFieldNo) {
		// TODO: Implement
		this.leftChild = leftChild;
		this.rightChild = rightChild;
		this.leftFieldNo = leftFieldNo;
		this.rightFieldNo = rightFieldNo;
		map = new HashMap<Key,List>();
		all_tuples = new  ArrayList<DBTuple>();
		
	}

	@Override
	public void open() {
		// TODO: Implement
		index = 0;
		leftChild.open();
		rightChild.open();
		innerJoin();

	}

	private void addToMap(Key thisKey,DBTuple tuple,boolean probeLeft) {
		
		if(!map.containsKey(thisKey)) {
			if(probeLeft) {
			ArrayList<List> outer_list = new ArrayList<List>();
			ArrayList<DBTuple> this_list = new ArrayList<DBTuple>();
			this_list.add(tuple);
			outer_list.add(this_list);
			map.put(thisKey, outer_list);
			}
		}
		else {
			if(probeLeft) {
			List<List> outer_list = map.get(thisKey);
			List<DBTuple> inner_list = outer_list.get(0);
			inner_list.add(tuple);
			}
			else {
				List<List> outer_list = map.get(thisKey);
				int sizeOuterList = outer_list.size();
				if(sizeOuterList==0) {
					System.out.println("unexpected, no value, but key present");
				}
				else if (sizeOuterList==1) {
					ArrayList<DBTuple> this_list = new ArrayList<DBTuple>();
					this_list.add(tuple);
					outer_list.add(this_list);
				}
				else if(sizeOuterList==2) {
					List<List> outer_list1 = map.get(thisKey);
					List<DBTuple> inner_list1 = outer_list1.get(1);
					inner_list1.add(tuple);
					
				}
				else
					System.out.println("unexpected length of outer list can't be more than 2");
				
			}
		}
	}
	
	private void innerJoin() {
		DBTuple tupleLeft;
		DBTuple tupleRight;
		do {
			tupleLeft = leftChild.next();
			if(tupleLeft.eof)break;
			Key<String> thisKey = new Key<String>(tupleLeft.getFieldAsString(leftFieldNo), tupleLeft.types[leftFieldNo]);
			addToMap(thisKey, tupleLeft, true);
			
		}while(!tupleLeft.eof);
		
		do {
			tupleRight = rightChild.next();
			if(tupleRight.eof)break;
			Key thisKey = new Key<String>((String)tupleRight.fields[rightFieldNo],tupleRight.types[rightFieldNo]);
			addToMap(thisKey, tupleRight, false);
		}while(!tupleRight.eof);
		
		
		for (HashMap.Entry<Key, List> entry:map.entrySet()) {
			List<List> outer_list = entry.getValue();
			if(outer_list.size()==2) {
				//both left and right sides present
				for(Object leftTuple:outer_list.get(0)) {
					for (Object rightTuple:outer_list.get(1)) {
						all_tuples.add(DBTuple.join ((DBTuple) leftTuple, (DBTuple) rightTuple));
						
					}
				}
			}
		}
	}
	
	@Override
	public DBTuple next() {
		// TODO: Implement
		if(index < all_tuples.size())
			return all_tuples.get(index++);
		else
			return new DBTuple();
	}

	@Override
	public void close() {
		// TODO: Implement
		index = 0;
		
	}
}
