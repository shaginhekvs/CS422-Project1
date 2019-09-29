package ch.epfl.dias.store.row;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;


public class RowStore extends Store {

	private class Row{
		Object[] fields;
		Row(Object[] fields){
			this.fields= fields;
		}
	}
	
	
	// TODO: Add required structures
	Vector<Row> rows ;
	DataType[] schema;
	String filename;
	String delimiter;
	public RowStore(DataType[] schema, String filename, String delimiter) {

		this.schema = schema;
		this.filename = filename;
		this.delimiter = delimiter;
		this.rows = new Vector<Row>();

	}

	@Override
	public void load() throws IOException {
		// TODO: Implement
		
		try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
		    String line;
		    while ((line = br.readLine()) != null) {
		        String[] values = line.split(delimiter);
		        Row r = new Row(values);
		        rows.add(r);
		    }
		    
		}
		catch(IOException e) {
			System.out.println("File"+filename+" not found for initializing rowstore, please try a valid path for the file");
			throw new IOException(e.getMessage());
		}
	}

	@Override
	public DBTuple getRow(int rownumber) {
		// TODO: Implement
		if(rownumber>=0 && rownumber < rows.size()) {
			return new DBTuple(this.rows.get(rownumber).fields,this.schema);
		}
		
		return new DBTuple();
		
		
	}
}
