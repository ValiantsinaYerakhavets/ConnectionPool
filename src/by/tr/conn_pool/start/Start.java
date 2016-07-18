package by.tr.conn_pool.start;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

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
			//System.out.println(result.getString(0));
		} 
		catch (SQLException e) 
		{
			e.printStackTrace();
		}
	

		pool.closeConnection(conn, st);
		pool.clearConnectionQueue();
		/*
		Connection conn = null;
		try
		{
			Class.forName("org.gjt.mm.mysql.Driver");
			conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1/poolDB", "root", "1111");
			System.out.println("ok");
			
			Statement st = conn.createStatement();
			int rows = st.executeUpdate("INSERT INTO users"
					+ "(Login, Password) VALUES "
					+ "(\"admin\", \"L. Tolstoy\")");
			System.out.println(rows);
		}
		catch(ClassNotFoundException e)
		{
			System.out.println("ClassNotFoundException");
		}
		catch(SQLException e)
		{
			System.out.println("SQLException");
			e.printStackTrace();
		}
		finally
		{
			try
			{
				if(conn!=null)
				{
					conn.close();
				}
			}
			catch(SQLException e)
			{
				System.out.println("SQLException");
			}
		}*/
	}
}
