package ch.epfl.dias.store.PAX;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.row.DBTuple;

public class PAXStore extends Store {

	// TODO: Add required structures
	private class Page{
		Object[] fields;
		Page(Object[] fields){
			this.fields = fields;
		}
	}
	ArrayList<Page> pages ;
	
	DataType[] schema;
	String filename;
	String delimiter;
	int tuplesPerPage ;
	int currentPage;
	int tuplesOnCurPage = 0;
	
	public PAXStore(DataType[] schema, String filename, String delimiter, int tuplesPerPage) {
		// TODO: Implement
		pages = new ArrayList<Page>();
		this.schema = schema;
		this.filename = filename;
		this.delimiter = delimiter;
		this.tuplesPerPage = tuplesPerPage;
		currentPage = -1;
	}
	
	private boolean insertTuple(String [] values) {
		if(currentPage <0 || tuplesOnCurPage >= tuplesPerPage) {
			//create new page
			Page newPage = new Page(new Object[schema.length*tuplesPerPage]);
			pages.add(newPage);
			currentPage ++ ;
			tuplesOnCurPage = 0;
		}
		
		if(values.length != schema.length) {
			System.out.println("Can't write because schema doesn't match data");
			return false;
		}
		
		for (int i=0;i<values.length;i++) {
			pages.get(currentPage).fields[i*tuplesPerPage+tuplesOnCurPage]=values[i];
		}
		tuplesOnCurPage++;
		return true;
		
	}
	
	private Object[] get_Tuple(int index , int page) {
		//System.out.println("index is "+index);
		//System.out.println("Page is"+page);
		if ( page >= this.pages.size() || (page == (this.pages.size()-1) && index >= this.tuplesOnCurPage))
			return null;
		
		Object[] fields = new Object[schema.length];
		
		Page pageById = pages.get(page);
		
		for (int i=index,j=0;i<schema.length*tuplesPerPage;i+=tuplesPerPage,j++) {
			fields[j] = pageById.fields[i];
		}
		
		
		return fields;
			
	}

	
	@Override
	public void load() throws IOException {
		// TODO: Implement
		try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
		    String line;
		    while ((line = br.readLine()) != null) {
		        String[] values = line.split(delimiter);
		        insertTuple(values);
		    }
		    
		}
		catch(IOException e) {
			System.out.println("File"+filename+" not found for initializing PAX, please try a valid path for the file");
			throw new IOException(e.getMessage());
		}

	}

	@Override
	public DBTuple getRow(int rownumber) {
		
		int pageID = rownumber / tuplesPerPage;
		int index = rownumber % tuplesPerPage;
		Object[] field = get_Tuple(index,pageID);
		if(field == null) {
			return new DBTuple();
		}
		// TODO: Implement
		DBTuple tuple = new DBTuple(field,this.schema);
		return tuple;
	}
}
