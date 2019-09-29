
import java.io.IOException;
import java.util.Arrays;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.PAX.PAXStore;
import ch.epfl.dias.store.column.ColumnStore;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;
import ch.epfl.dias.store.row.RowStore;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.Timeout;

public class TestAll {

	DataType[] orderSchema;
	DataType[] lineitemSchema;
	DataType[] schema;
	int standardVectorsize = 10;
	int pageSize = 10;
	ColumnStore columnstoreData;
	ColumnStore columnstoreOrder;
	ColumnStore columnstoreLineItem;
	
	RowStore rowstoreData;
    RowStore rowstoreOrder;
    RowStore rowstoreLineItem;
    
    int colToSelect = 5;
    int valueToCompare = 30000;
       
    PAXStore paxstoreData;
    PAXStore paxstoreOrder;
    PAXStore paxstoreLineItem;
    int [] colsSelect ;

    String path = "C:/Users/Dell/Documents/courses/2019/semA/DB/big/";
	@Before
	public void init() throws IOException {
		colsSelect = new int[2];
	    colsSelect[0] = 0;
	    colsSelect[1] = 1;
	    
		schema = new DataType[] { DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.INT,
				DataType.INT, DataType.INT, DataType.INT, DataType.INT };

		orderSchema = new DataType[] { DataType.INT, DataType.INT, DataType.STRING, DataType.DOUBLE, DataType.STRING,
				DataType.STRING, DataType.STRING, DataType.INT, DataType.STRING };

		lineitemSchema = new DataType[] { DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.DOUBLE,
				DataType.DOUBLE, DataType.DOUBLE, DataType.DOUBLE, DataType.STRING, DataType.STRING, DataType.STRING,
				DataType.STRING, DataType.STRING, DataType.STRING, DataType.STRING, DataType.STRING };

		columnstoreData = new ColumnStore(schema, "input/data.csv", ",");
		//columnstoreData = new ColumnStore(schema, path+"data.csv", ",");
		columnstoreData.load();

		columnstoreOrder = new ColumnStore(orderSchema, "input/orders_small.csv", "\\|");
		//columnstoreOrder = new ColumnStore(orderSchema, path+"orders_big.csv", "\\|");
		
		columnstoreOrder.load();

		columnstoreLineItem = new ColumnStore(lineitemSchema, "input/lineitem_small.csv", "\\|");
		//columnstoreLineItem = new ColumnStore(lineitemSchema, path+"lineitem_big.csv", "\\|");
		
		columnstoreLineItem.load();
		
		paxstoreData = new PAXStore(schema, "input/data.csv", ",", 3);
        
        paxstoreData.load();
        
        //paxstoreOrder = new PAXStore(orderSchema, "input/orders_small.csv", "\\|", 3);
        paxstoreOrder = new PAXStore(orderSchema, path+"orders_big.csv", "\\|", 3);
        paxstoreOrder.load();
        
        //paxstoreLineItem = new PAXStore(lineitemSchema, "input/lineitem_small.csv", "\\|", 3);
        paxstoreLineItem = new PAXStore(lineitemSchema, path+"lineitem_big.csv", "\\|", 3);
        
        paxstoreLineItem.load();
        
        rowstoreData = new RowStore(schema, "input/data.csv", ",");
        rowstoreData.load();
        
        rowstoreOrder = new RowStore(orderSchema, "input/orders_small.csv", "\\|");
        rowstoreOrder = new RowStore(orderSchema, path+"orders_big.csv", "\\|");
        rowstoreOrder.load();
        
        rowstoreLineItem = new RowStore(lineitemSchema, "input/lineitem_small.csv", "\\|");
        rowstoreLineItem = new RowStore(lineitemSchema, path+"lineitem_big.csv", "\\|");
        rowstoreLineItem.load();
        
	}
	
	@Test
	public void paxspTestLineItem(){
	    //SELECT COUNT(*) FROM data WHERE col0 == 3 
		long startTime = System.nanoTime();
		long endTime = System.nanoTime();

		long duration = (endTime - startTime);  //divide by 1000000 to get milliseconds.
		for(int i=1;i<=1000;i+=100) {
		startTime = System.nanoTime();
		paxstoreOrder = new PAXStore(orderSchema, path+"orders_big.csv", "\\|", i);
        try {
		paxstoreOrder.load();
        }
        catch(Exception e) {
        }
	    ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(paxstoreOrder);
	    ch.epfl.dias.ops.volcano.Select sel = new ch.epfl.dias.ops.volcano.Select(scan, BinaryOp.GE, colToSelect, this.valueToCompare);
	    ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(sel, Aggregate.AVG, DataType.INT, 2);
	
		agg.open();
		
		// This query should return only one result
		DBTuple result = agg.next();
		int output = result.getFieldAsInt(0);
		System.out.println(output);
		endTime = System.nanoTime();
		duration = (endTime - startTime)/1000000;  //divide by 1000000 to get milliseconds.
		System.out.println("time for "+i+"is: "+ duration);
		}
	}
	
	@Test
	public void paxsJOIN(){
	    //SELECT COUNT(*) FROM data WHERE col0 == 3     
		
	    ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(paxstoreLineItem);
	    ch.epfl.dias.ops.volcano.Scan scan1 = new ch.epfl.dias.ops.volcano.Scan(paxstoreOrder);
	    ch.epfl.dias.ops.volcano.Scan scan2 = new ch.epfl.dias.ops.volcano.Scan(paxstoreOrder);
	    ch.epfl.dias.ops.volcano.HashJoin join = new ch.epfl.dias.ops.volcano.HashJoin( scan1,scan,0,0);
	    ch.epfl.dias.ops.volcano.HashJoin join2 = new ch.epfl.dias.ops.volcano.HashJoin( scan2,join,0,0);
	    
	    ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(join2, Aggregate.COUNT, DataType.INT, 0);
	    
	
		agg.open();
		
		// This query should return only one result
		DBTuple result = agg.next();
		int output = result.getFieldAsInt(0);
		System.out.println(output);
	}
	
	@Test
	public void paxsTestLineItem(){
	    //SELECT COUNT(*) FROM data WHERE col0 == 3     
		
	    ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(paxstoreLineItem);
	    ch.epfl.dias.ops.volcano.Select sel = new ch.epfl.dias.ops.volcano.Select(scan, BinaryOp.GE, colToSelect, this.valueToCompare);
	    ch.epfl.dias.ops.volcano.Project agg = new ch.epfl.dias.ops.volcano.Project(sel,colsSelect);
	
		agg.open();
		
		// This query should return only one result
		DBTuple result = agg.next();
		while(!result.eof)result = agg.next();

	}
	
	
	@Test
	public void colSJoin() {
		/* SELECT COUNT(*) FROM data WHERE col0 == 3 */
		ch.epfl.dias.ops.columnar.Scan scan = new ch.epfl.dias.ops.columnar.Scan(columnstoreLineItem);
		ch.epfl.dias.ops.columnar.Scan scan1 = new ch.epfl.dias.ops.columnar.Scan(columnstoreOrder);
		ch.epfl.dias.ops.columnar.Scan scan2 = new ch.epfl.dias.ops.columnar.Scan(columnstoreOrder);
		ch.epfl.dias.ops.columnar.Join join = new ch.epfl.dias.ops.columnar.Join(scan1, scan ,0,0);
		ch.epfl.dias.ops.columnar.Join join2 = new ch.epfl.dias.ops.columnar.Join(join, scan2 ,0,0);
		ch.epfl.dias.ops.columnar.ProjectAggregate agg = new ch.epfl.dias.ops.columnar.ProjectAggregate(join2, Aggregate.COUNT,
				DataType.INT, 0);

		DBColumn[] result = agg.execute();

		
		
	}
	@Test
	public void colSpTestLineItem() {
		/* SELECT COUNT(*) FROM data WHERE col0 == 3 */
		ch.epfl.dias.ops.columnar.Scan scan = new ch.epfl.dias.ops.columnar.Scan(columnstoreLineItem);

		ch.epfl.dias.ops.columnar.Select sel = new ch.epfl.dias.ops.columnar.Select(scan, BinaryOp.GE, colToSelect, this.valueToCompare);
		ch.epfl.dias.ops.columnar.ProjectAggregate agg = new ch.epfl.dias.ops.columnar.ProjectAggregate(sel, Aggregate.AVG,
				DataType.INT, 2);

		DBColumn[] result = agg.execute();

		
		
	}
	
	@Test
	public void colSTestLineItem() {
		/* SELECT COUNT(*) FROM data WHERE col0 == 3 */
		ch.epfl.dias.ops.columnar.Scan scan = new ch.epfl.dias.ops.columnar.Scan(columnstoreLineItem);

		ch.epfl.dias.ops.columnar.Select sel = new ch.epfl.dias.ops.columnar.Select(scan, BinaryOp.GE, colToSelect, this.valueToCompare);
		ch.epfl.dias.ops.columnar.Project agg = new ch.epfl.dias.ops.columnar.Project(sel, colsSelect);

		DBColumn[] result = agg.execute();

		// This query should return only one result
		int output = result[0].getAsInteger()[0];
		System.out.println(output+" splineitem");
		assertTrue(output == 6);
		
		
	}
	
	@Test
	public void rowJoin(){
	    /* SELECT COUNT(*) FROM data WHERE col0 == 3 */	    
	    ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);
	    ch.epfl.dias.ops.volcano.Scan scan1 = new ch.epfl.dias.ops.volcano.Scan(rowstoreOrder);
	    ch.epfl.dias.ops.volcano.Scan scan2 = new ch.epfl.dias.ops.volcano.Scan(rowstoreOrder);
	    
	    ch.epfl.dias.ops.volcano.HashJoin join = new ch.epfl.dias.ops.volcano.HashJoin(scan1,scan,0,0);
	    ch.epfl.dias.ops.volcano.HashJoin join1 = new ch.epfl.dias.ops.volcano.HashJoin(scan2,join,0,0);
	    
	    ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(join1, Aggregate.COUNT, DataType.INT, 0);
	
		agg.open();
		
		// This query should return only one result
		DBTuple result = agg.next();
		int output = result.getFieldAsInt(0);
		System.out.println("spTestLineItem "+output);
	}
	
	@Test
	public void rowSpTestLineItem(){
	    /* SELECT COUNT(*) FROM data WHERE col0 == 3 */	    
	    ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);
	    ch.epfl.dias.ops.volcano.Select sel = new ch.epfl.dias.ops.volcano.Select(scan, BinaryOp.GE, colToSelect, this.valueToCompare);
	    ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(sel, Aggregate.AVG, DataType.INT, 2);
	
		agg.open();
		
		// This query should return only one result
		DBTuple result = agg.next();
		int output = result.getFieldAsInt(0);
		System.out.println("spTestLineItem "+output);
		assertTrue(output == 6);
	}
	
	@Test
	public void rowSTestLineItem(){
	    /* SELECT COUNT(*) FROM data WHERE col0 == 3 */	    
	    ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);
	    ch.epfl.dias.ops.volcano.Select sel = new ch.epfl.dias.ops.volcano.Select(scan, BinaryOp.GE, colToSelect, this.valueToCompare);
	    ch.epfl.dias.ops.volcano.Project agg = new ch.epfl.dias.ops.volcano.Project(sel, colsSelect);
	
		agg.open();
		
		// This query should return only one result
		DBTuple result = agg.next();
		while(!result.eof)result = agg.next();
	}
	
	@Test
	public void vectorJoin() {
		/* SELECT COUNT(*) FROM data WHERE col0 == 3 */
		
		ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreLineItem, standardVectorsize);
		ch.epfl.dias.ops.vector.Scan scan1 = new ch.epfl.dias.ops.vector.Scan(columnstoreOrder, standardVectorsize);
		ch.epfl.dias.ops.vector.Scan scan2 = new ch.epfl.dias.ops.vector.Scan(columnstoreOrder, standardVectorsize);
		
		ch.epfl.dias.ops.vector.Join join = new ch.epfl.dias.ops.vector.Join(scan1, scan,0,0);
		ch.epfl.dias.ops.vector.Join join1 = new ch.epfl.dias.ops.vector.Join(join, scan2,0,0);
		
		
		ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(join1,
				Aggregate.COUNT, DataType.INT, 0);

		agg.open();
		DBColumn[] result = agg.next();

		// This query should return only one result
		int output = result[0].getAsInteger()[0];
		System.out.println("sptestlineitem "+output);
		
	}
	@Test
	public void avectorSpTestLineItem() {
		/* SELECT COUNT(*) FROM data WHERE col0 == 3 */
		long startTime = System.nanoTime();
		long endTime = System.nanoTime();

		long duration = (endTime - startTime);  //divide by 1000000 to get milliseconds.
		for(int i=1;i<=1000;i+=100) {
		startTime = System.nanoTime();
		ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreLineItem, i);
		ch.epfl.dias.ops.vector.Select sel = new ch.epfl.dias.ops.vector.Select(scan, BinaryOp.GE, colToSelect, this.valueToCompare);
		ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(sel,
				Aggregate.AVG, DataType.INT, 2);

		agg.open();
		DBColumn[] result = agg.next();

		// This query should return only one result
		int output = result[0].getAsInteger()[0];
		System.out.println("sptestlineitem "+output);
		endTime = System.nanoTime();
		duration = startTime-endTime;
		System.out.println("vector test time "+i+"is:"+duration);
		}
		
	}
	
	
	//@Test
	public void vectorSTestLineItem() {
		/* SELECT COUNT(*) FROM data WHERE col0 == 3 */
		
		ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreLineItem, standardVectorsize);
		ch.epfl.dias.ops.vector.Select sel = new ch.epfl.dias.ops.vector.Select(scan, BinaryOp.GE, colToSelect, this.valueToCompare);
		ch.epfl.dias.ops.vector.Project agg = new ch.epfl.dias.ops.vector.Project(sel,this.colsSelect);

		agg.open();
		DBColumn[] result = agg.next();

		// This query should return only one result
		while(!result[0].eof)result = agg.next();
		
	}

	
	
	
	
}
