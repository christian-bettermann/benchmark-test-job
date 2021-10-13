package net.atos.ignite.services.benchmarktest;

import java.sql.*;
import java.text.SimpleDateFormat;

public class Main {
	static final String DB_URL = System.getenv("DB_URL");
	static final String USER = System.getenv("USER");
	static final String PASSWORD = System.getenv("PASSWORD");
	static final String RESULTS_DB_URL = System.getenv("RESULTS_DB_URL");
	static final String RESULTS_USER = System.getenv("RESULTS_USER"); 
	static final String RESULTS_PASSWORD = System.getenv("RESULTS_PASSWORD"); 
	static final String DATA_SCALEFACTOR = System.getenv("DATA_SCALEFACTOR");
	static final String SETUP_TYPE = System.getenv("SETUP_TYPE");
	static final String SETUP_SIZE = System.getenv("SETUP_SIZE");
	
	static String pricingSummaryReportQuery;
	static String minimumCostSupplierQuery;
	static String shippingPriorityQuery;
	static String suppliersWhoKeptOrdersWaitingQuery;
	
	
	
	
	
	public static void main(String[] args) {
		init();
		// ------- Insert the queries here -----------
		try {
			switch(SETUP_TYPE) {
				case "POSTGRES":
					pricingSummaryReportQuery(0, "01 12 1995");
					pricingSummaryReportQuery(-30, "01 12 1996");
					pricingSummaryReportQuery(-60, "01 12 1997");
					pricingSummaryReportQuery(-90, "01 12 1998");
					pricingSummaryReportQuery(-120, "01 12 1999");
				
					minimumCostSupplierQuery(15, "%STEEL%", "EUROPE");
					minimumCostSupplierQuery(12, "%COPPER%", "EUROPE");
					minimumCostSupplierQuery(29, "%POLISHED TIN%", "EUROPE");
					minimumCostSupplierQuery(15, "%STEEL%", "AMERICA");
					minimumCostSupplierQuery(12, "%COPPER%", "AMERICA");
					minimumCostSupplierQuery(29, "%POLISHED TIN%", "AMERICA");
					                  
					shippingPriorityQuery("BUILDING", "01 06 1998");
					shippingPriorityQuery("AUTOMOBILE", "01 06 1997");
					shippingPriorityQuery("FURNITURE", "01 06 1996");
					shippingPriorityQuery("HOUSEHOLD", "01 06 1995");
					shippingPriorityQuery("BUILDING", "01 12 1997");
					shippingPriorityQuery("AUTOMOBILE", "01 12 1996");
					shippingPriorityQuery("FURNITURE", "01 12 1995");
					shippingPriorityQuery("HOUSEHOLD", "01 12 1994");
					
		  			suppliersWhoKeptOrdersWaitingQuery('F', "GERMANY"); //all lineitems shipped
		  			suppliersWhoKeptOrdersWaitingQuery('O', "GERMANY"); //no lineitems shipped
		  			suppliersWhoKeptOrdersWaitingQuery('P', "GERMANY"); //some lineitems shipped
		  			suppliersWhoKeptOrdersWaitingQuery('F', "CANADA");
		  			suppliersWhoKeptOrdersWaitingQuery('O', "CANADA");
		  			suppliersWhoKeptOrdersWaitingQuery('P', "CANADA");
		  			suppliersWhoKeptOrdersWaitingQuery('F', "UNITED STATES");
		  			suppliersWhoKeptOrdersWaitingQuery('O', "UNITED STATES");
		  			suppliersWhoKeptOrdersWaitingQuery('P', "UNITED STATES");
		  			suppliersWhoKeptOrdersWaitingQuery('F', "FRANCE");
		  			suppliersWhoKeptOrdersWaitingQuery('O', "FRANCE");
		  			suppliersWhoKeptOrdersWaitingQuery('P', "FRANCE");
		  			break;
                case "IGNITE-IN-MEMORY-DB":
                case "IGNITE-IN-MEMORY-CACHE":
					pricingSummaryReportQuery(0, "1995-12-01");
					pricingSummaryReportQuery(-30, "1996-12-01");
					pricingSummaryReportQuery(-60, "1997-12-01");
					pricingSummaryReportQuery(-90, "1998-12-01");
					pricingSummaryReportQuery(-120, "1999-12-01");
				
					minimumCostSupplierQuery(15, "%STEEL%", "EUROPE");
					minimumCostSupplierQuery(12, "%COPPER%", "EUROPE");
					minimumCostSupplierQuery(29, "%POLISHED TIN%", "EUROPE");
					minimumCostSupplierQuery(15, "%STEEL%", "AMERICA");
					minimumCostSupplierQuery(12, "%COPPER%", "AMERICA");
					minimumCostSupplierQuery(29, "%POLISHED TIN%", "AMERICA");
					                  
					shippingPriorityQuery("BUILDING", "1998-06-01");
					shippingPriorityQuery("AUTOMOBILE", "1997-06-01");
					shippingPriorityQuery("FURNITURE", "1996-06-01");
					shippingPriorityQuery("HOUSEHOLD", "1995-06-01");
					shippingPriorityQuery("BUILDING", "1997-12-01");
					shippingPriorityQuery("AUTOMOBILE", "1996-12-01");
					shippingPriorityQuery("FURNITURE", "1995-12-01");
					shippingPriorityQuery("HOUSEHOLD", "1994-12-01");
					
		  			suppliersWhoKeptOrdersWaitingQuery('F', "GERMANY"); //all lineitems shipped
		  			suppliersWhoKeptOrdersWaitingQuery('O', "GERMANY"); //no lineitems shipped
		  			suppliersWhoKeptOrdersWaitingQuery('P', "GERMANY"); //some lineitems shipped
		  			suppliersWhoKeptOrdersWaitingQuery('F', "CANADA");
		  			suppliersWhoKeptOrdersWaitingQuery('O', "CANADA");
		  			suppliersWhoKeptOrdersWaitingQuery('P', "CANADA");
		  			suppliersWhoKeptOrdersWaitingQuery('F', "UNITED STATES");
		  			suppliersWhoKeptOrdersWaitingQuery('O', "UNITED STATES");
		  			suppliersWhoKeptOrdersWaitingQuery('P', "UNITED STATES");
		  			suppliersWhoKeptOrdersWaitingQuery('F', "FRANCE");
		  			suppliersWhoKeptOrdersWaitingQuery('O', "FRANCE");
		  			suppliersWhoKeptOrdersWaitingQuery('P', "FRANCE");
					break;
				default:
					System.out.println("Invalid SETUP declared!");
					break;
			}
		// -------------------------------------------
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

	public Main() {
		
	}

	public static void init() {
		try {
			Class.forName("org.apache.ignite.IgniteJdbcThinDriver");
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
        }
        		
		switch(SETUP_TYPE) {
		case "POSTGRES":	
			pricingSummaryReportQuery = "SELECT l_returnflag, l_linestatus, SUM(l_quantity) AS sum_qty, SUM(l_extendedprice) AS sum_base_price, SUM(l_extendedprice * ( 1 - l_discount )) AS sum_disc_price, SUM(l_extendedprice * ( 1 - l_discount ) * ( 1 + l_tax )) AS sum_charge, Avg(l_quantity) AS avg_qty, Avg(l_extendedprice) AS avg_price, Avg(l_discount) AS avg_disc, Count(*) AS count_order FROM lineitem WHERE  l_shipdate <= to_date(?, 'DD MM YYYY') + ? GROUP  BY l_returnflag, l_linestatus ORDER  BY l_returnflag, l_linestatus; ";
			minimumCostSupplierQuery = "SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment FROM part, supplier, partsupp, nation, region WHERE  p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND p_size = ? AND p_type LIKE ? AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = ? AND ps_supplycost = (SELECT Min(ps_supplycost) FROM   partsupp, supplier, nation, region WHERE  p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = ?) ORDER  BY s_acctbal DESC, n_name, s_name, p_partkey LIMIT 100;";
			shippingPriorityQuery = "SELECT l_orderkey, SUM(l_extendedprice * ( 1 - l_discount )) AS revenue, o_orderdate, o_shippriority FROM   customer, orders, lineitem WHERE  c_mktsegment = ? AND c_custkey = o_custkey AND l_orderkey = o_orderkey AND o_orderdate < to_date(?, 'DD MM YYYY') AND l_shipdate > to_date(?, 'DD MM YYYY') GROUP BY l_orderkey, o_orderdate, o_shippriority ORDER BY revenue DESC, o_orderdate LIMIT 10;";
			suppliersWhoKeptOrdersWaitingQuery = "SELECT s_name, Count(*) AS numwait FROM supplier, lineitem l1, orders, nation WHERE s_suppkey = l1.l_suppkey AND o_orderkey = l1.l_orderkey AND o_orderstatus = ? AND l1.l_receiptdate > l1.l_commitdate AND EXISTS (SELECT * FROM lineitem l2 WHERE  l2.l_orderkey = l1.l_orderkey AND l2.l_suppkey <> l1.l_suppkey) AND NOT EXISTS (SELECT * FROM lineitem l3 WHERE  l3.l_orderkey = l1.l_orderkey AND l3.l_suppkey <> l1.l_suppkey AND l3.l_receiptdate > l3.l_commitdate) AND s_nationkey = n_nationkey AND n_name = ? GROUP  BY s_name ORDER  BY numwait DESC, s_name LIMIT 100;";
			break;
		case "IGNITE-IN-MEMORY-DB":
			pricingSummaryReportQuery = "SELECT l_returnflag, l_linestatus, SUM(l_quantity) AS sum_qty, SUM(l_extendedprice) AS sum_base_price, SUM(l_extendedprice * ( 1 - l_discount )) AS sum_disc_price, SUM(l_extendedprice * ( 1 - l_discount ) * ( 1 + l_tax )) AS sum_charge, Avg(l_quantity) AS avg_qty, Avg(l_extendedprice) AS avg_price, Avg(l_discount) AS avg_disc, Count(*) AS count_order FROM lineitem WHERE  l_shipdate <= Dateadd('DAY', ?, ?) GROUP  BY l_returnflag, l_linestatus ORDER  BY l_returnflag, l_linestatus; ";
			minimumCostSupplierQuery = "SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment FROM part, supplier, partsupp, nation, region WHERE  p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND p_size = ? AND p_type LIKE ? AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = ? AND ps_supplycost = (SELECT Min(ps_supplycost) FROM   partsupp, supplier, nation, region WHERE  p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = ?) ORDER  BY s_acctbal DESC, n_name, s_name, p_partkey LIMIT 100;";
			shippingPriorityQuery = "SELECT l_orderkey, SUM(l_extendedprice * ( 1 - l_discount )) AS revenue, o_orderdate, o_shippriority FROM   customer, orders, lineitem WHERE  c_mktsegment = ? AND c_custkey = o_custkey AND l_orderkey = o_orderkey AND o_orderdate < ? AND l_shipdate > ? GROUP BY l_orderkey, o_orderdate, o_shippriority ORDER BY revenue DESC, o_orderdate LIMIT 10;";
			suppliersWhoKeptOrdersWaitingQuery = "SELECT s_name, Count(*) AS numwait FROM supplier, lineitem l1, orders, nation WHERE s_suppkey = l1.l_suppkey AND o_orderkey = l1.l_orderkey AND o_orderstatus = ? AND l1.l_receiptdate > l1.l_commitdate AND EXISTS (SELECT * FROM lineitem l2 WHERE  l2.l_orderkey = l1.l_orderkey AND l2.l_suppkey <> l1.l_suppkey) AND NOT EXISTS (SELECT * FROM lineitem l3 WHERE  l3.l_orderkey = l1.l_orderkey AND l3.l_suppkey <> l1.l_suppkey AND l3.l_receiptdate > l3.l_commitdate) AND s_nationkey = n_nationkey AND n_name = ? GROUP  BY s_name ORDER  BY numwait DESC, s_name LIMIT 100;";
			break;
		case "IGNITE-IN-MEMORY-CACHE":
			pricingSummaryReportQuery = "SELECT l_returnflag, l_linestatus, SUM(l_quantity) AS sum_qty, SUM(l_extendedprice) AS sum_base_price, SUM(l_extendedprice * ( 1 - l_discount )) AS sum_disc_price, SUM(l_extendedprice * ( 1 - l_discount ) * ( 1 + l_tax )) AS sum_charge, Avg(l_quantity) AS avg_qty, Avg(l_extendedprice) AS avg_price, Avg(l_discount) AS avg_disc, Count(*) AS count_order FROM \"LineitemCache\".lineitem WHERE  l_shipdate <= Dateadd('DAY', ?, ?) GROUP  BY l_returnflag, l_linestatus ORDER  BY l_returnflag, l_linestatus; ";
			minimumCostSupplierQuery = "SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment FROM \"PartCache\".part, \"SupplierCache\".supplier, \"PartsuppCache\".partsupp, \"NationCache\".nation, \"RegionCache\".region WHERE  p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND p_size = ? AND p_type LIKE ? AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = ? AND ps_supplycost = (SELECT Min(ps_supplycost) FROM  \"PartsuppCache\".partsupp, \"SupplierCache\".supplier, \"NationCache\".nation, \"RegionCache\".region WHERE  p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = ?) ORDER  BY s_acctbal DESC, n_name, s_name, p_partkey LIMIT 100;";
			shippingPriorityQuery = "SELECT l_orderkey, SUM(l_extendedprice * ( 1 - l_discount )) AS revenue, o_orderdate, o_shippriority FROM   \"CustomerCache\".customer, \"OrdersCache\".orders, \"LineitemCache\".lineitem WHERE  c_mktsegment = ? AND c_custkey = o_custkey AND l_orderkey = o_orderkey AND o_orderdate < ? AND l_shipdate > ? GROUP BY l_orderkey, o_orderdate, o_shippriority ORDER BY revenue DESC, o_orderdate LIMIT 10;";
			suppliersWhoKeptOrdersWaitingQuery = "SELECT s_name, Count(*) AS numwait FROM \"SupplierCache\".supplier, \"LineitemCache\".lineitem l1, \"OrdersCache\".orders, \"NationCache\".nation WHERE s_suppkey = l1.l_suppkey AND o_orderkey = l1.l_orderkey AND o_orderstatus = ? AND l1.l_receiptdate > l1.l_commitdate AND EXISTS (SELECT * FROM \"LineitemCache\".lineitem l2 WHERE  l2.l_orderkey = l1.l_orderkey AND l2.l_suppkey <> l1.l_suppkey) AND NOT EXISTS (SELECT * FROM \"LineitemCache\".lineitem l3 WHERE  l3.l_orderkey = l1.l_orderkey AND l3.l_suppkey <> l1.l_suppkey AND l3.l_receiptdate > l3.l_commitdate) AND s_nationkey = n_nationkey AND n_name = ? GROUP  BY s_name ORDER  BY numwait DESC, s_name LIMIT 100;";
			break;
        	default:
            		pricingSummaryReportQuery = "";
            		minimumCostSupplierQuery = "";
			shippingPriorityQuery = "";
			suppliersWhoKeptOrdersWaitingQuery = "";
			break;
		}
	}

	public static void pricingSummaryReportQuery(int dayInterval, String date) {
        String query = pricingSummaryReportQuery;
        int ctr = 0;
		long finishQuery = 0;
		long finishOutput = 0;
		
        try (Connection con = DriverManager.getConnection(DB_URL, USER, PASSWORD);
             PreparedStatement pst = con.prepareStatement(query)) {
        	switch(SETUP_TYPE) {
			case "POSTGRES":
	        	pst.setString(1, date);
	        	pst.setInt(2, dayInterval);    
	        	break;
            case "IGNITE-IN-MEMORY-DB":
            case "IGNITE-IN-MEMORY-CACHE":
				pst.setInt(1, dayInterval);
				pst.setDate(2, java.sql.Date.valueOf(date));			
				break;
			default:
				break;
        	}
            long start = System.currentTimeMillis();
            ResultSet rs = pst.executeQuery();

			if (rs.next()) {
				finishQuery = System.currentTimeMillis();
				ctr++;
			}
			while (rs.next()) {
				ctr++;
			}
			finishOutput = System.currentTimeMillis();
			long timeResponse = finishQuery - start;
			long timeOutput = finishOutput - start;
			if(ctr > 0) {
				writeToResultsDatabase(start, "pricingSummaryReportQuery", "Date: "+ date + "; DayInterval: " + dayInterval + ";", timeResponse, timeOutput, ctr, Integer.parseInt(DATA_SCALEFACTOR), SETUP_TYPE, SETUP_SIZE);
				//System.out.println("Query: pricingSummaryReportQuery with params date: " + date + ", dayInterval: " + dayInterval + "; found " + ctr + " rows; Time for request: " + timeResponse + "ms;  Time for output: " + timeOutput + "ms; Timestamp taken on start: "+ start +";");
			} else {
				System.out.println("Query failed! Query: pricingSummaryReportQuery with params date: " + date + ", dayInterval: " + dayInterval + "; Timestamp taken on start: "+ start +";");
			}
			con.close();
        } catch (Exception e) {
        	e.printStackTrace();
        }
	}

	public static void minimumCostSupplierQuery(int partSize, String typeLike, String regionName) {
		String query = minimumCostSupplierQuery;
		int ctr = 0;
		long finishQuery = 0;
		long finishOutput = 0;
		try (Connection con = DriverManager.getConnection(DB_URL, USER, PASSWORD);
             PreparedStatement pst = con.prepareStatement(query)) {
			pst.setInt(1, partSize);
            pst.setString(2, typeLike);
            pst.setString(3, regionName);
            pst.setString(4, regionName);   
            long start = System.currentTimeMillis();
            ResultSet rs = pst.executeQuery();

			if (rs.next()) {
				finishQuery = System.currentTimeMillis();
				ctr++;
			}
			while (rs.next()) {
				ctr++;
			}
			finishOutput = System.currentTimeMillis();
			long timeResponse = finishQuery - start;
			long timeOutput = finishOutput - start;
			if(ctr > 0) {
				writeToResultsDatabase(start, "minimumCostSupplierQuery", "partSize: "+ partSize + "; typeLike: " + typeLike + "; regionName " + regionName + ";", timeResponse, timeOutput, ctr, Integer.parseInt(DATA_SCALEFACTOR), SETUP_TYPE, SETUP_SIZE);
				//System.out.println("Query: minimumCostSupplierQuery with params partSize: "+ partSize + "; typeLike: " + typeLike + "; regionName " + regionName + "; found " + ctr + " rows; Time for request: " + timeResponse + "ms;  Time for output: " + timeOutput + "ms; Timestamp taken on start: "+ start +";");
			} else {
				System.out.println("Query failed! Query: minimumCostSupplierQuery with params partSize: "+ partSize + "; typeLike: " + typeLike + "; regionName " + regionName + "; Timestamp taken on start: "+ start +";");
			}
			con.close();
        } catch (Exception e) {
        	e.printStackTrace();
        }	
	}

	public static void shippingPriorityQuery(String marktSegment, String date) {
		String query = shippingPriorityQuery;
		int ctr = 0;
		long finishQuery = 0;
		long finishOutput = 0;
		try (Connection con = DriverManager.getConnection(DB_URL, USER, PASSWORD);
             PreparedStatement pst = con.prepareStatement(query)) {
			switch(SETUP_TYPE) {
			case "POSTGRES":
				pst.setString(1, marktSegment);
	            pst.setString(2, date);
	            pst.setString(3, date);  
	        	break;
            case "IGNITE-IN-MEMORY-DB":
            case "IGNITE-IN-MEMORY-CACHE":
				pst.setString(1, marktSegment);
	            pst.setDate(2, java.sql.Date.valueOf(date));
	            pst.setDate(3, java.sql.Date.valueOf(date));
				break;
			default:
				break;
        	}
            long start = System.currentTimeMillis();
            ResultSet rs = pst.executeQuery();

			if (rs.next()) {
				finishQuery = System.currentTimeMillis();
				ctr++;
			}
			while (rs.next()) {
				ctr++;
			}
			finishOutput = System.currentTimeMillis();
			long timeResponse = finishQuery - start;
			long timeOutput = finishOutput - start;
			if(ctr > 0) {
				writeToResultsDatabase(start, "shippingPriorityQuery", "marktSegment: "+ marktSegment + "; date: " + date + ";", timeResponse, timeOutput, ctr, Integer.parseInt(DATA_SCALEFACTOR), SETUP_TYPE, SETUP_SIZE);
				//System.out.println("Query: shippingPriorityQuery with params marktSegment: "+ marktSegment + "; date: " + date + "; found " + ctr + " rows; Time for request: " + timeResponse + "ms;  Time for output: " + timeOutput + "ms; Timestamp taken on start: "+ start +";");
			} else {
				System.out.println("Query failed! Query: shippingPriorityQuery with params marktSegment: "+ marktSegment + "; date: " + date + "; Timestamp taken on start: "+ start +";");
			}
			con.close();
        } catch (Exception e) {
        	e.printStackTrace();
        }		
	}
	
	public static void suppliersWhoKeptOrdersWaitingQuery(char orderstatus, String nation) {
		String query = suppliersWhoKeptOrdersWaitingQuery;
		int ctr = 0;
		long finishQuery = 0;
		long finishOutput = 0;
		try (Connection con = DriverManager.getConnection(DB_URL, USER, PASSWORD);
             PreparedStatement pst = con.prepareStatement(query)) {
            pst.setString(1, String.valueOf(orderstatus));
            pst.setString(2, nation);
            long start = System.currentTimeMillis();
            ResultSet rs = pst.executeQuery();

			if (rs.next()) {
				finishQuery = System.currentTimeMillis();
				ctr++;
			}
			while (rs.next()) {
				ctr++;
			}
			finishOutput = System.currentTimeMillis();
			long timeResponse = finishQuery - start;
			long timeOutput = finishOutput - start;
			if(ctr > 0) {
				writeToResultsDatabase(start, "suppliersWhoKeptOrdersWaitingQuery", "orderstatus: "+ orderstatus + "; nation: " + nation + ";", timeResponse, timeOutput, ctr, Integer.parseInt(DATA_SCALEFACTOR), SETUP_TYPE, SETUP_SIZE);
				//System.out.println("Query: suppliersWhoKeptOrdersWaitingQuery with params orderstatus: "+ orderstatus + "; nation: " + nation + "; found " + ctr + " rows; Time for request: " + timeResponse + "ms;  Time for output: " + timeOutput + "ms; Timestamp taken on start: "+ start +";");
			} else {
				System.out.println("Query failed! Query: suppliersWhoKeptOrdersWaitingQuery with params orderstatus: "+ orderstatus + "; nation: " + nation + "; Timestamp taken on start: "+ start +";");
			}
			con.close();
        } catch (Exception e) {
        	e.printStackTrace();
        }	
	}
	
	public static void writeToResultsDatabase(long timestamp, String query_name, String parameters, long response_millis, long output_millis, long row_count, int data_scalefactor, String setup_type, String setup_size) {
        String query = "INSERT INTO results(timestamp, query_name, parameters, response_millis, output_millis, row_count, data_scalefactor, setup_type, setup_size) VALUES(?, ?, ?, ?, ?, ?, ?, ?,?);";
        System.out.println(timestamp +", "+ query_name+", "+parameters+", "+response_millis+", "+output_millis+", "+row_count+", "+data_scalefactor+", "+setup_type+", "+setup_size);
        try (Connection con = DriverManager.getConnection(RESULTS_DB_URL, RESULTS_USER, RESULTS_PASSWORD);
                PreparedStatement pst = con.prepareStatement(query)) {
        	   con.setAutoCommit(true);
       	   
        	   
        	   pst.setLong(1, timestamp);
               pst.setString(2, query_name);
               pst.setString(3, parameters);
               pst.setLong(4, response_millis);
               pst.setLong(5, output_millis);
               pst.setLong(6, row_count);
               pst.setInt(7, data_scalefactor);
               pst.setString(8, setup_type);
               pst.setString(9, setup_size);
               int response = pst.executeUpdate();
               
               System.out.println("Added " + response + " row to the results database.");
               con.close();
        } catch (Exception e) {
        	e.printStackTrace();
        }
	}
}
