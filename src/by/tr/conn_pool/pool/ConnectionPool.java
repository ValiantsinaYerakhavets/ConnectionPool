package by.tr.library.dao.pool;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ConnectionPool
{
	private final static Logger LOG = LogManager.getRootLogger();
	
	private static ConnectionPool instance = null;
	private static boolean isInit = false;
	
	private BlockingQueue<Connection> available;
	private BlockingQueue<Connection> givenAway;
	
	private String driverName;
	private String url;
	private String user;
	private String password;
	private int poolSize;
	
	public static ConnectionPool getInstance()
	{
		if(instance==null)
		{
			instance = new ConnectionPool();
		}
		if(!isInit)
		{
			try 
			{
				instance.initPool();
				isInit = true;
			} 
			catch (ConnectionPoolException e) 
			{
				e.printStackTrace();
			}
		}
		return instance;
	}
	
	private ConnectionPool()
	{		
		DBResourseManager resourseManager = DBResourseManager.getInstance();
		this.driverName = resourseManager.getValue(DBParameter.DB_DRIVER);
		this.url = resourseManager.getValue(DBParameter.DB_URL);
		this.user = resourseManager.getValue(DBParameter.DB_USER);
		this.password = resourseManager.getValue(DBParameter.DB_PASSWORD);
		
		try
		{
			this.poolSize = Integer.parseInt(resourseManager.getValue(DBParameter.DB_POOL_SIZE));
		}
		catch(NumberFormatException e)
		{
			poolSize = 5;
		}
	}
	
	private void initPool() throws ConnectionPoolException
	{
		Locale.setDefault(Locale.ENGLISH);
		
		try
		{
			Class.forName(driverName);
			givenAway = new ArrayBlockingQueue<Connection>(poolSize);
			available = new ArrayBlockingQueue<Connection>(poolSize);
			
			for(int i=0; i<poolSize; i++)
			{
				Connection connection = DriverManager.getConnection(url, user, password);
				available.add(connection);
			}
			isInit = true;
		}
		catch(SQLException e)
		{
			LOG.error("Error in initing the pool! " + e.getStackTrace());
		}
		catch(ClassNotFoundException e)
		{
			LOG.error("Cannot find database! " + e.getStackTrace());
		}
	}
	
	public Connection takeConnection() throws ConnectionPoolException
	{
		Connection conn = null;
		try
		{
			conn = available.take();
			givenAway.add(conn);
		}
		catch(InterruptedException e)
		{
			LOG.error("Error connecting to the datasource!" + e.getStackTrace());
		}
		return conn;
	}
	
	public void returnConnection(Connection connection) throws ConnectionPoolException, SQLException
	{
		if(connection.isClosed())
		{
			throw new SQLException("Attempting to return closed connection!");
		}
		if(connection.isReadOnly())
		{
			connection.setReadOnly(false);
		}
		boolean remove = givenAway.remove(connection);
		if(!remove)
		{
			throw new SQLException("Error deleting from queue!");
		}
		boolean offer = available.offer(connection);
		if(!offer)
		{
			throw new SQLException("Error allocating connection in the pool!");
		}
	}
	
	public void closeConnection(Connection conn, Statement st, ResultSet rs)
	{
		try
		{
			conn.close();
		}
		catch(SQLException e)
		{
			LOG.error("Connection isn't returned to the pool! " + e.getStackTrace());
		}
		
		try
		{
			rs.close();
		}
		catch(SQLException e)
		{
			e.printStackTrace();
			LOG.error("ResultSet isn't closed! " + e.getStackTrace());
		}
		
		try
		{
			st.close();
		}
		catch(SQLException e)
		{
			e.printStackTrace();
			LOG.error("Statement isn't closed! " + e.getStackTrace());
		}
	}
	
	public void closeConnection(Connection conn, Statement st)
	{
		try
		{
			conn.close();
		}
		catch(SQLException e)
		{
			LOG.error("Connection isn't returned to the pool! " + e.getStackTrace());
		}
		
		try
		{
			st.close();
		}
		catch(SQLException e)
		{
			LOG.error("Statement isn't closed! " + e.getStackTrace());
		}
	}
	
	public void dispose()
	{
		this.clearConnectionQueue();
	}
	
	private void clearConnectionQueue()
	{
		try
		{
			closeConnectionQueue(givenAway);
			closeConnectionQueue(available);
		}
		catch(SQLException e)
		{
			LOG.error("Error closing the connection! " + e.getStackTrace());
		}
	}
	
	private void closeConnectionQueue(BlockingQueue<Connection> queue) throws SQLException
	{
		Connection conn;
		while((conn = queue.poll()) != null)
		{
			if(!conn.getAutoCommit())
			{
				conn.commit();
			}
			conn.close();
		}
	}

}
