package ch.epfl.dias.store.column;

import java.util.ArrayList;

public class DBColumnIndex extends DBColumn {

	public ArrayList<Integer> indices ;
	public DBColumnIndex() {
		indices = new ArrayList<Integer>();
	}
	
	public void insertUntil(int start,int end) {
		for (int i=start;i<=end;i++)
			indices.add(i);
	}
	public void removeIndex(int index) {
		indices.remove(index);
	}
	
}
