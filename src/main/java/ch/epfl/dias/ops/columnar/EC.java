package ch.epfl.dias.ops.columnar;

import java.util.ArrayList;
import java.util.Vector;

import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.column.DBColumnIndex;

public class EC {
	
	public static DBColumn[] commitEager(DBColumn [] selectedData,Scan[] stores) {
		//after late execution, filter all the columns and drop index column


		DBColumn [] data;
		DBColumnIndex dataIndex=null;
		DBColumnIndex colIndex=null;
		int k =0 ;
		ArrayList<DBColumn> newCols = new ArrayList<DBColumn>();
		for(int i=0,j=0;i<selectedData.length;i+=2,j++) {
			k = 0;
			dataIndex = (DBColumnIndex) selectedData[i];
			colIndex = (DBColumnIndex) selectedData[i+1];
			int thisColumnSize = colIndex.indices.size();
			
			// get data for current table
			int [] columnsIDs = new int[thisColumnSize];
			for (int col:colIndex.indices) {
				columnsIDs[k++] = col;
			}
			
			data = stores[j].getColumns(columnsIDs);

			for(int curColIndex:colIndex.indices) { // loop to make all columns needed for this table
			
				DBColumn newCol;
				if(!data[curColIndex].eof) {
					newCol = new DBColumn(new Vector<Object>(),data[curColIndex].type,i);
					for (int curRowindex:dataIndex.indices) {
						newCol.fields.add(data[curColIndex].fields.get(curRowindex));
					}
				}
				else {
					newCol = new DBColumn();
				}
	
				newCols.add(newCol);
			}
		}
		return newCols.toArray(new DBColumn[newCols.size()]);
	}
}
