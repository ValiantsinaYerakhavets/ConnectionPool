package by.tr.conn_pool.start;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import by.tr.conn_pool.pool.ConnectionPool;
import by.tr.conn_pool.pool.ConnectionPoolException;

public class Start 
{
	public static void main(String[] args)
	{
		ConnectionPool pool = null;
		Connection conn = null;
		Statement st = null;
		try 
		{
			pool = ConnectionPool.getInstance();
			conn = pool.takeConnection();
		} 
		catch (ConnectionPoolException e) 
		{
			e.printStackTrace();
		}
	
		ResultSet result = null;
		try 
		{
			st = conn.createStatement();
			result = st.executeQuery("SELECT * from users");
			while(result.next())
			{
				String login= result.getString("Login");
				String password = result.getString("password");
				boolean isBlocked = result.getBoolean("isBlocked");
				System.out.println(login + "\t" + password + "\t" + isBlocked);
			}
		} 
		catch (SQLException e) 
		{
			e.printStackTrace();
		}
	

		try 
		{
			pool.returnConnection(conn);
		} 
		catch (ConnectionPoolException | SQLException e) 
		{
			e.printStackTrace();
		}
		pool.dispose();
	}
}
