package by.tr.conn_pool.pool;

public final class DBParameter
{
	private DBParameter(){}
	
	public static final String DB_DRIVER = "org.gjt.mm.mysql.Driver";
	public static final String DB_URL = "jdbc:mysql://127.0.0.1/poolDB";
	public static final String DB_USER = "root";
	public static final String DB_PASSWORD = "1111";
	public static final int DB_POOL_SIZE = 5;
}