package ch.epfl.dias.ops.vector;



import java.util.ArrayList;
import java.util.Vector;

import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.column.DBColumnIndex;
import ch.epfl.dias.ops.columnar.Scan;
public class EC {
	
	public static DBColumn[] commitEager(DBColumn [] selectedData) {
		//after late execution, filter all the columns and drop index column
		DBColumnIndex dataIndex = (DBColumnIndex) selectedData[0];
		DBColumn[] columns = new DBColumn[selectedData.length-1];
		int i =0;
		for ( i=0;i<columns.length;i++)
		{
			columns[i] = new DBColumn(new Vector<Object>(),selectedData[i+1].type,i);
		}
		for (int j:dataIndex.indices)
		{
			for (i=0;i<columns.length;i++) {
				
			columns[i].fields.add(selectedData[i+1].fields.get(j));
		
			}
		}
		return columns;
	}
}
