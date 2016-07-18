package by.tr.conn_pool.pool;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ConnectionPool
{
	private final static Logger LOG = LogManager.getRootLogger();
	
	private BlockingQueue<Connection> available;
	private BlockingQueue<Connection> givenAway;
	
	private String driverName;
	private String url;
	private String user;
	private String password;
	private int poolSize;
	
	private ConnectionPool()
	{
		DBResourceManager dbResourseManager = DBResourceManager.getInstance();
		
		this.driverName = dbResourseManager.getValue(DBParameter.DB_DRIVER);
		this.url = dbResourseManager.getValue(DBParameter.DB_URL);
		this.user = dbResourseManager.getValue(DBParameter.DB_USER);
		this.password = dbResourseManager.getValue(DBParameter.DB_PASSWORD);
		
		try
		{
			this.poolSize = Integer.parseInt(dbResourseManager.getValue(DBParameter.DB_PASSWORD));
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
				Connection conn = DriverManager.getConnection(url, user, password);
				PooledConnection pooledConnection = new PooledConnection(conn);
				available.add(pooledConnection);
			}
		}
		catch(SQLException e)
		{
			throw new ConnectionPoolException("", e);
		}
		catch(ClassNotFoundException e)
		{
			LOG.error("Cannot find database!");
		}
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
			LOG.error("Error closing the connection!");
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
			LOG.error("Error connecting to the datasource!");
		}
		return conn;
	}
	
	public void closeConnection(Connection conn, Statement st, ResultSet rs)
	{
		try
		{
			conn.close();
		}
		catch(SQLException e)
		{
			LOG.error("Connection isn't returned to the pool!");
		}
		
		try
		{
			rs.close();
		}
		catch(SQLException e)
		{
			LOG.error("ResultSet isn't closed!");
		}
		
		try
		{
			st.close();
		}
		catch(SQLException e)
		{
			LOG.error("Statement isn't closed!");
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
			LOG.error("Connection isn't returned to the pool!");
		}
		
		try
		{
			st.close();
		}
		catch(SQLException e)
		{
			LOG.error("Statement isn't closed!");
		}
	}
	
	public void closeConnectionQueue(BlockingQueue<Connection> queue) throws SQLException
	{
		Connection conn;
		while((conn = queue.poll()) != null)
		{
			if(!conn.getAutoCommit())
			{
				conn.commit();
			}
			((PooledConnection) conn).reallyClose();
		}
	}
	
	private class PooledConnection implements Connection
	{
		private Connection connection;
		
		public PooledConnection(Connection conn) throws SQLException
		{
			this.connection = conn;
			this.connection.setAutoCommit(true);
		}
		
		public void reallyClose() throws SQLException
		{
			this.connection.close();
		}
		
		@Override
		public boolean isWrapperFor(Class<?> arg0) throws SQLException 
		{
			return connection.isWrapperFor(arg0);
		}

		@Override
		public <T> T unwrap(Class<T> arg0) throws SQLException 
		{
			return connection.unwrap(arg0);
		}

		@Override
		public void abort(Executor arg0) throws SQLException 
		{
			connection.abort(arg0);
		}

		@Override
		public void clearWarnings() throws SQLException 
		{
			connection.clearWarnings();
		}

		@Override
		public void close() throws SQLException 
		{
			if(connection.isClosed())
			{
				throw new SQLException("Attempting to close closed connection!");
			}
			if(connection.isReadOnly())
			{
				connection.setReadOnly(false);
			}
			if(!givenAway.remove(this))
			{
				throw new SQLException("Error deleting from queue!");
			}
			if(!givenAway.offer(connection))
			{
				throw new SQLException("Error allocating connection in the pool!");
			}
		}

		@Override
		public void commit() throws SQLException 
		{
			connection.commit();
		}

		@Override
		public Array createArrayOf(String arg0, Object[] arg1) throws SQLException 
		{
			return connection.createArrayOf(arg0, arg1);
		}

		@Override
		public Blob createBlob() throws SQLException 
		{
			return connection.createBlob();
		}

		@Override
		public Clob createClob() throws SQLException 
		{
			return connection.createClob();
		}

		@Override
		public NClob createNClob() throws SQLException 
		{
			return connection.createNClob();
		}

		@Override
		public SQLXML createSQLXML() throws SQLException 
		{
			return connection.createSQLXML();
		}

		@Override
		public Statement createStatement() throws SQLException 
		{
			return connection.createStatement();
		}

		@Override
		public Statement createStatement(int arg0, int arg1) throws SQLException
		{
			return connection.createStatement(arg0, arg1);
		}

		@Override
		public Statement createStatement(int arg0, int arg1, int arg2) throws SQLException 
		{
			return connection.createStatement(arg0, arg1, arg2);
		}

		@Override
		public Struct createStruct(String arg0, Object[] arg1) throws SQLException 
		{
			return connection.createStruct(arg0, arg1);
		}

		@Override
		public boolean getAutoCommit() throws SQLException 
		{
			return connection.getAutoCommit();
		}

		@Override
		public String getCatalog() throws SQLException 
		{
			return connection.getCatalog();
		}

		@Override
		public Properties getClientInfo() throws SQLException 
		{
			return connection.getClientInfo();
		}

		@Override
		public String getClientInfo(String arg0) throws SQLException
		{
			return connection.getClientInfo(arg0);
		}

		@Override
		public int getHoldability() throws SQLException 
		{
			return connection.getHoldability();
		}

		@Override
		public DatabaseMetaData getMetaData() throws SQLException 
		{
			return connection.getMetaData();
		}

		@Override
		public int getNetworkTimeout() throws SQLException 
		{
			return connection.getNetworkTimeout();
		}

		@Override
		public String getSchema() throws SQLException 
		{
			return connection.getSchema();
		}

		@Override
		public int getTransactionIsolation() throws SQLException
		{
			return connection.getTransactionIsolation();
		}

		@Override
		public Map<String, Class<?>> getTypeMap() throws SQLException 
		{
			return connection.getTypeMap();
		}

		@Override
		public SQLWarning getWarnings() throws SQLException 
		{
			return connection.getWarnings();
		}

		@Override
		public boolean isClosed() throws SQLException 
		{
			return connection.isClosed();
		}

		@Override
		public boolean isReadOnly() throws SQLException 
		{
			return connection.isReadOnly();
		}

		@Override
		public boolean isValid(int timeout) throws SQLException 
		{
			return connection.isValid(timeout);
		}

		@Override
		public String nativeSQL(String sql) throws SQLException
		{
			return connection.nativeSQL(sql);
		}

		@Override
		public CallableStatement prepareCall(String sql) throws SQLException
		{
			return connection.prepareCall(sql);
		}

		@Override
		public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
				throws SQLException
		{
			return connection.prepareCall(sql, resultSetType, resultSetConcurrency);
		}

		@Override
		public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
				int resultSetHoldability) throws SQLException 
		{
			return connection.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
		}

		@Override
		public PreparedStatement prepareStatement(String sql) throws SQLException 
		{
			return connection.prepareStatement(sql);
		}

		@Override
		public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException 
		{
			return connection.prepareStatement(sql, autoGeneratedKeys);
		}

		@Override
		public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException
		{
			return connection.prepareStatement(sql, columnIndexes);
		}

		@Override
		public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException 
		{
			return connection.prepareStatement(sql, columnNames);
		}

		@Override
		public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
				throws SQLException 
		{
			return connection.prepareStatement(sql, resultSetType, resultSetConcurrency);
		}

		@Override
		public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
				int resultSetHoldability) throws SQLException 
		{
			return connection.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
		}

		@Override
		public void releaseSavepoint(Savepoint savepoint) throws SQLException 
		{
			connection.releaseSavepoint(savepoint);
		}

		@Override
		public void rollback() throws SQLException 
		{
			connection.rollback();
		}

		@Override
		public void rollback(Savepoint savepoint) throws SQLException 
		{
			connection.rollback(savepoint);
		}

		@Override
		public void setAutoCommit(boolean autoCommit) throws SQLException 
		{
			connection.setAutoCommit(autoCommit);
		}

		@Override
		public void setCatalog(String catalog) throws SQLException 
		{
			connection.setCatalog(catalog);
		}

		@Override
		public void setClientInfo(Properties properties) throws SQLClientInfoException 
		{
			connection.setClientInfo(properties);
		}

		@Override
		public void setClientInfo(String name, String value) throws SQLClientInfoException 
		{
			connection.setClientInfo(name, value);
		}

		@Override
		public void setHoldability(int holdability) throws SQLException 
		{
			connection.setHoldability(holdability);
		}

		@Override
		public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException 
		{
			connection.setNetworkTimeout(executor, milliseconds);
		}

		@Override
		public void setReadOnly(boolean readOnly) throws SQLException 
		{
			connection.setReadOnly(readOnly);
		}

		@Override
		public Savepoint setSavepoint() throws SQLException 
		{
			return connection.setSavepoint();
		}

		@Override
		public Savepoint setSavepoint(String name) throws SQLException 
		{
			return connection.setSavepoint(name);
		}

		@Override
		public void setSchema(String schema) throws SQLException
		{
			connection.setSchema(schema);
		}

		@Override
		public void setTransactionIsolation(int level) throws SQLException
		{
			connection.setTransactionIsolation(level);
		}

		@Override
		public void setTypeMap(Map<String, Class<?>> map) throws SQLException 
		{
			connection.setTypeMap(map);
		}
	}
}
