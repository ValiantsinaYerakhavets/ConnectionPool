package by.tr.conn_pool.pool;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class poolTest 
{
	ConnectionPool pool = null;
	Connection conn = null;
	Statement st = null;
	
	@BeforeClass
	void initPool()
	{
		try 
		{
			pool = ConnectionPool.getInstance();
			conn = pool.takeConnection();
		} 
		catch (ConnectionPoolException e) 
		{
			e.printStackTrace();
		}
	}
	
	@Test
	void testPool()
	{
		ResultSet result = null;
		try 
		{
			st = conn.createStatement();
			result = st.executeQuery("SELECT * from users");
			//System.out.println(result.getString(0));
		} 
		catch (SQLException e) 
		{
			e.printStackTrace();
		}
	}
	
	@AfterClass
	void closePool() throws SQLException
	{
		pool.closeConnection(conn, st);
		pool.clearConnectionQueue();
	}
}
