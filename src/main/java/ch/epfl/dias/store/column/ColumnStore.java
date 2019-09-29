package ch.epfl.dias.store.column;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Vector;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;

public class ColumnStore extends Store {

	

	
	Vector<DBColumn> columns;
	
	DataType[] schema;
	String filename;
	String delimiter;
	public boolean lateMaterialization;
	public int numRows = 0;

	public ColumnStore(DataType[] schema, String filename, String delimiter) {
		this(schema, filename, delimiter, false);
	}

	public ColumnStore(DataType[] schema, String filename, String delimiter, boolean lateMaterialization) {
		// TODO: Implement
		this.schema = schema;
		this.filename = filename;
		this.delimiter = delimiter;
		this.columns = new Vector<DBColumn>();
		this.lateMaterialization = lateMaterialization;
	}

	
	@Override
	public void load() throws IOException {
		// TODO: Implement
		boolean isFirstLine = true;
		int countRows = 0;
		try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
		    String line;
		    while ((line = br.readLine()) != null) {
		    	countRows++;
		        String[] values = line.split(delimiter);
		        if(isFirstLine) {
		        	for ( int i=0;i<values.length;i++) {
		        		columns.add(new DBColumn(new Vector<Object>(),schema[i],i));
		        	}
		        	isFirstLine=false;
		        }
		        
		        for (int i=0;i<values.length;i++) {
		        	columns.get(i).fields.add((Object)values[i]);
		        }
		    }
		    
		}
		catch(IOException e) {
			System.out.println("File"+filename+" not found for initializing columnstore, please try a valid path for the file");
			throw new IOException(e.getMessage());
		}
		numRows = countRows;
	}

	@Override
	public DBColumn[] getColumns(int[] columnsToGet) {
		// TODO: Implement
		DBColumn[] ret_columns = new DBColumn[columnsToGet.length];
		int j=0;
		for (int i :columnsToGet) {
			if(i>=0 && i< this.columns.size()) {
				ret_columns[j++] = this.columns.get(i);
			}
			else {
				ret_columns[j++] = new DBColumn();
			}
		}
		return ret_columns;
	}
	
	
	public int getNumColumns() {
		return columns.size();
	}
	
	public int getNumRows() {
		return numRows;
	}
}
