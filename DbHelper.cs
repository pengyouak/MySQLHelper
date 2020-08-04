/* ***********************************************************************
 * 版本：2020.8.4
 * 说明：查询时填充DataSet
 *
 * 版本：2020.7.31
 * 说明：数据库初始化选项增加是否初始化连接池的选项
 *
 * 版本：2020.7.24
 * 说明：移除自定义Command对象
 *       移除弃用的事务方法
 *
 * 版本：2020.7.8
 * 说明：移除自定义Command对象，调整事务执行参数
 *       释放连接方式统一处理
 *       增加单连接的数据库对象构造函数
 *       数据库对象增加ID和Key属性
 *       数据库对象的连接池改为非静态（可创建多个连接池）
 *********************************************************************** */

using MySql.Data.MySqlClient;
using NLog;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;

namespace MaterialBasic.Utils
{
    /// <summary>
    /// DBHelper
    /// </summary>
    public class DBHelper
    {
        private static MySqlDB _mySqlDB;

        /// <summary>
        /// Mysql数据库操作对象
        /// </summary>
        public static MySqlDB MySqlDB
        {
            get
            {
                if (_mySqlDB == null)
                    _mySqlDB = new MySqlDB(_dbOptions);
                return _mySqlDB;
            }
        }

        private static DBOptions _dbOptions;

        /// <summary>
        /// 通过配置实例化DBHelper
        /// </summary>
        /// <param name="dbOptions">配置项</param>
        public DBHelper(DBOptions dbOptions)
        {
            _dbOptions = dbOptions;
        }

        public static string EscapeString(string str)
        {
            if (string.IsNullOrEmpty(str)) return str;

            return MySqlHelper.EscapeString(str);
        }
    }

    /// <summary>
    /// 公共DBHelper返回结果
    /// </summary>
    public class DBResult : PageResult
    {
        /// <summary>
        /// 是否成功
        /// </summary>
        public bool IsSuccessed { get; set; }

        /// <summary>
        /// DataTable
        /// </summary>
        public DataTable Table { get; set; }

        /// <summary>
        /// DataSet
        /// </summary>
        public DataSet DataSet { get; set; }

        /// <summary>
        /// Object
        /// </summary>
        public Object DataObject { get; set; }

        /// <summary>
        /// 错误消息
        /// </summary>
        public string Message { get; set; }

        /// <summary>
        /// 受影响行数
        /// </summary>
        public int AffectRowCount { get; set; } = 0;
    }

    /// <summary>
    /// 分页结果集扩展
    /// </summary>
    public class PageResult
    {
        private int _currentPage = 1;
        private int _totalPages = 1;
        private int _pageSize = 10;
        private long _totalRecords = 0;

        /// <summary>
        /// 当前页
        /// </summary>
        public int CurrentPage
        {
            get { return _currentPage; }
            set
            {
                if (value < 1)
                    _currentPage = 1;
                else if (value > _totalPages)
                    _currentPage = _totalPages;
                else
                    _currentPage = value;
            }
        }

        /// <summary>
        /// 总页数
        /// </summary>
        public int TotalPages
        {
            get
            {
                if (_totalPages == 0)
                    return 1;
                else
                    return _totalPages;
            }
        }

        /// <summary>
        /// 页大小
        /// </summary>
        public int PageSize
        {
            get { return _pageSize; }
            set
            {
                if (_pageSize <= 0)
                    _pageSize = 10;
                else
                    _pageSize = value;
                _totalPages = (int)Math.Ceiling(_totalRecords / (double)PageSize);
            }
        }

        /// <summary>
        /// 总记录数
        /// </summary>
        public long TotalRecords
        {
            get { return _totalRecords; }
            set
            {
                _totalRecords = value;
                _totalPages = (int)Math.Ceiling(_totalRecords / (double)PageSize);
            }
        }

        /// <summary>
        /// 分页排序的自定义字段
        /// </summary>
        public string PageBy { get; set; }

        /// <summary>
        /// 是否时升序
        /// </summary>
        public bool IsAsc { get; set; }

        /// <summary>
        /// 构造函数
        /// </summary>
        public PageResult() { }

        /// <summary>
        /// 构造函数
        /// </summary>
        /// <param name="pageSize">页大小</param>
        /// <param name="totalRecords">总记录数</param>
        public PageResult(int pageSize, long totalRecords)
        {
            this.CurrentPage = 1;
            this._pageSize = PageSize;
            this._totalRecords = TotalRecords;
            _totalPages = (int)Math.Ceiling(TotalRecords / (double)PageSize);
        }

        /// <summary>
        /// 构造函数
        /// </summary>
        /// <param name="currPage">当前页</param>
        /// <param name="pageSize">页大小</param>
        /// <param name="totalRecords">总记录数</param>
        public PageResult(int currPage, int pageSize, long totalRecords) :
            this(pageSize, totalRecords)
        {
            this.CurrentPage = currPage;
        }
    }

    /// <summary>
    /// 数据库连接池管理对象
    /// </summary>
    public class PooledConnection
    {
        private NLog.Logger _logger;
        private MySqlConnection _mConnection = null;// 数据库连接
        private bool _mBBusy = false; // 此连接是否正在使用的标志，默认没有正在使用

        /// <summary>
        /// 构造函数，根据一个 Connection 构告一个 PooledConnection 对象
        /// </summary>
        /// <param name="connection">MySqlConnection</param>
        public PooledConnection(ref MySqlConnection connection)
        {
            _logger = NLog.LogManager.GetCurrentClassLogger();
            _mConnection = connection;
        }

        /// <summary>
        /// 返回此对象中的连接
        /// </summary>
        /// <returns></returns>
        public MySqlConnection GetConnection()
        {
            return _mConnection;
        }

        /// <summary>
        /// 设置此对象的，连接
        /// </summary>
        /// <param name="connection">MySqlConnection</param>
        public void SetConnection(ref MySqlConnection connection)
        {
            _mConnection = connection;
        }

        /// <summary>
        /// 获得对象连接是否忙
        /// </summary>
        public bool IsBusy
        {
            get { return _mBBusy; }
            set { _mBBusy = value; }
        }
    }

    /// <summary>
    /// 数据库连接池
    /// </summary>
    public class ConnectionPool : IDisposable
    {
        private string _testTable = ""; // 测试连接是否可用的测试表名，默认没有测试表
        private int _initialConnections = 10; // 连接池的初始大小
        private int _incrementalConnections = 5;// 连接池自动增加的大小
        private int _maxConnections = 50; // 连接池最大的大小
        private List<PooledConnection> _mPooledconnections = null; // 存放连接池中数据库连接的向量
        private string _mySqlConnection = "";//ConfigurationManager.ConnectionStrings["ConnectSql"].ConnectionString;
        private static object objectLocker = new object();
        private NLog.Logger _logger;

        /// <summary>
        /// 构造函数
        /// </summary>
        /// <param name="options">数据库连接字配置</param>
        private ConnectionPool(DBOptions options)
        {
            _logger = NLog.LogManager.GetCurrentClassLogger();
            _mySqlConnection = options.ConnectionString;
            _initialConnections = options.InitialConnections;
            _incrementalConnections = options.IncrementalConnections;
            _maxConnections = options.MaxConnections;
        }

        /// <summary>
        /// 创建一个数据库连接池，连接池中的可用连接的数量采用类成员 initialConnections 中设置的值
        /// </summary>
        public ConnectionPool CreatePool()
        {
            // 如果己经创建，则返回
            if (_mPooledconnections != null)
                return this;

            _mPooledconnections = new List<PooledConnection>();
            // 根据 initialConnections 中设置的值，创建连接。
            CreateConnections(this._initialConnections);

            return this;
        }

        public static ConnectionPool CreateConnectionPoolInstance(DBOptions options)
        {
            return new ConnectionPool(options);
        }

        /// <summary>
        /// 连接池名称
        /// </summary>
        public string PoolName { get; set; }

        /// <summary>
        /// 获取或设置连接池的初始大小
        /// </summary>
        public int InitialConnections
        {
            get { return this._initialConnections; }
            set { this._initialConnections = value; }
        }

        /// <summary>
        /// 获取或设置连接池自动增加的大小
        /// </summary>
        public int IncrementalConnections
        {
            get { return this._incrementalConnections; }
            set { this._incrementalConnections = value; }
        }

        /// <summary>
        /// 获取或设置连接池中最大的可用连接数量
        /// </summary>
        public int MaxConnections
        {
            get { return this._maxConnections; }
            set { this._maxConnections = value; }
        }

        /// <summary>
        /// 获取或设置测试数据库表的名字
        /// </summary>
        public String TestTable
        {
            get { return this._testTable; }
            set { this._testTable = value; }
        }

        /// <summary>
        /// 获取工作中的连接数量
        /// </summary>
        /// <returns></returns>
        public int GetBusyConnectionsCount()
        {
            return _mPooledconnections.Count(x => x.IsBusy);
        }

        /// <summary>
        /// 获取空闲的连接数量
        /// </summary>
        /// <returns></returns>
        public int GetFreeConnectionsCount()
        {
            return _mPooledconnections.Count(x => !x.IsBusy);
        }

        /// <summary>
        /// 创建由 numConnections 指定数目的数据库连接 , 并把这些连接 放入 m_Pooledconnections
        /// </summary>
        /// <param name="numConnections"></param>
        private void CreateConnections(int numConnections)
        {
            // 循环创建指定数目的数据库连接
            for (int x = 0; x < numConnections; x++)
            {
                if (this._maxConnections > 0 && this._mPooledconnections.Count >= this._maxConnections)
                {
                    _logger.Trace($"连接数已经达到最大值无法追加.");
                    break;
                }
                _logger.Trace($"追加了 1 个连接.");
                try
                {
                    MySqlConnection tmpConnect = NewConnection();
                    _mPooledconnections.Add(new PooledConnection(ref tmpConnect));
                }
                catch (Exception ex)
                {
                    _logger.Error(ex, $"追加连接 {x + 1} 异常");
                    throw ex;
                }
            }
        }

        /// <summary>
        /// 创建一个新的数据库连接并返回它
        /// </summary>
        /// <returns></returns>
        private MySqlConnection NewConnection()
        {
            // 创建一个数据库连接
            MySqlConnection conn = new MySqlConnection(_mySqlConnection);
            conn.Open();
            return conn;
        }

        /// <summary>
        /// 通过调用 getFreeConnection() 函数返回一个可用的数据库连接 , 如果当前没有可用的数据库连接，并且更多的数据库连接不能创
        /// </summary>
        /// <returns></returns>
        public MySqlConnection GetConnection()
        {
            // 确保连接池己被创建
            if (_mPooledconnections == null)
                return null; // 连接池还没创建，则返回 null

            lock (objectLocker)
            {
                MySqlConnection conn = GetFreeConnection(); // 获得一个可用的数据库连接
                                                            // 如果目前没有可以使用的连接，即所有的连接都在使用中
                while (conn == null)
                {
                    Thread.Sleep(250);
                    conn = GetFreeConnection(); // 重新再试，直到获得可用的连接，如果
                }
                return conn;
            }
        }

        /// <summary>
        /// 本函数从连接池向量 connections 中返回一个可用的的数据库连接，如果 当前没有可用的数据库连接，本函数则根据 incrementalConnections 设置
        /// 的值创建几个数据库连接，并放入连接池中。 如果创建后，所有的连接仍都在使用中，则返回 null
        /// </summary>
        /// <returns></returns>
        private MySqlConnection GetFreeConnection()
        {
            // 从连接池中获得一个可用的数据库连接
            MySqlConnection conn = FindFreeConnection();
            if (conn == null)
            {
                CreateConnections(_incrementalConnections);
                // 重新从池中查找是否有可用连接
                conn = FindFreeConnection();
            }
            _logger.Trace($"分配了一个数据库连接.");
            _logger.Trace($"当前工作中的连接数：{GetBusyConnectionsCount()}, 空闲的连接数：{GetFreeConnectionsCount()}, 最大连接数：{MaxConnections}");
            return conn;
        }

        /// <summary>
        /// 查找连接池中所有的连接，查找一个可用的数据库连接， 如果没有可用的连接，返回 null
        /// </summary>
        /// <returns></returns>
        private MySqlConnection FindFreeConnection()
        {
            lock (objectLocker)
            {
                MySqlConnection conn = null;
                // 遍历所有的对象，看是否有可用的连接
                for (int i = 0; i < _mPooledconnections.Count; ++i)
                {
                    if (!_mPooledconnections[i].IsBusy)
                    {
                        conn = _mPooledconnections[i].GetConnection();
                        _mPooledconnections[i].IsBusy = true;
                        // 测试此连接是否可用
                        if (!TestConnection(ref conn))
                        {
                            // 如果此连接不可再用了，则创建一个新的连接， 并替换此不可用的连接对象，如果创建失败，返回 null
                            try
                            {
                                conn = NewConnection();
                            }
                            catch (Exception ex)
                            {
                                throw ex;
                            }
                            _mPooledconnections[i].SetConnection(ref conn);
                        }
                        break; // 己经找到一个可用的连接，退出
                    }
                }
                return conn;// 返回找到到的可用连接
            }
        }

        /// <summary>
        /// 测试一个连接是否可用，如果不可用，关掉它并返回 false 否则可用返回 true
        /// </summary>
        /// <param name="conn"></param>
        /// <returns></returns>
        private bool TestConnection(ref MySqlConnection conn)
        {
            try
            {
                if (conn.State != ConnectionState.Open)
                    return false;
                //查询系统表
                string sql = "select 1;";
                using (MySqlCommand dbComm = new MySqlCommand(sql, conn))
                {
                    dbComm.ExecuteNonQuery();
                }
            }
            catch (Exception ex)
            {
                _logger.Error(ex, "数据库连接失败.." + ex.Message);
                // 上面抛出异常，此连接己不可用，关闭它，并返回 false;
                CloseConnection(conn);
                return false;
            }
            return true;
        }

        /// <summary>
        /// 此函数返回一个数据库连接到连接池中，并把此连接置为空闲。 所有使用连接池获得的数据库连接均应在不使用此连接时返回它。
        /// </summary>
        /// <param name="conn"></param>
        public void ReturnConnection(MySqlConnection conn)
        {
            // 确保连接池存在，如果连接没有创建（不存在），也负责关闭指定连接
            if (_mPooledconnections == null || !_mPooledconnections.Any(x => x.GetConnection() == conn))
            {
                conn.Close();
                conn.Dispose();
                return;
            }
            lock (objectLocker)
            {
                var temp = _mPooledconnections.Find(x => x.GetConnection() == conn);
                if (temp != null)
                {
                    _logger.Trace($"回收了一个数据库连接.");
                    temp.IsBusy = false;
                }
            }
            _logger.Trace($"当前工作中的连接数：{GetBusyConnectionsCount()}, 空闲的连接数：{GetFreeConnectionsCount()}, 最大连接数：{MaxConnections}");
        }

        /// <summary>
        /// 刷新连接池中所有的连接对象
        /// </summary>
        public void RefreshConnections()
        {
            // 确保连接池己创新存在
            if (_mPooledconnections == null)
                return;

            lock (objectLocker)
            {
                for (int i = 0; i < _mPooledconnections.Count; ++i)
                {
                    if (_mPooledconnections[i].IsBusy)
                        Thread.Sleep(5000); //等待5s

                    // 关闭此连接，用一个新的连接代替它。
                    CloseConnection(_mPooledconnections[i].GetConnection());
                    MySqlConnection tmpConnect = NewConnection();
                    _mPooledconnections[i].SetConnection(ref tmpConnect);
                    _mPooledconnections[i].IsBusy = false;
                }
            }
        }

        /// <summary>
        /// 关闭连接池中所有的连接，并清空连接池。
        /// </summary>
        public void CloseConnectionPool()
        {
            // 确保连接池己创新存在
            if (_mPooledconnections == null)
                return;

            for (int i = 0; i < _mPooledconnections.Count; ++i)
            {
                if (_mPooledconnections[i].IsBusy)
                    Thread.Sleep(5000); //等待5s

                // 关闭此连接，用一个新的连接代替它。
                CloseConnection(_mPooledconnections[i].GetConnection());
                _mPooledconnections.RemoveAt(i);
            }
            _mPooledconnections.Clear();
            _mPooledconnections = null;
        }

        /// <summary>
        /// 关闭一个数据库连接
        /// </summary>
        /// <param name="conn"></param>
        private void CloseConnection(MySqlConnection conn)
        {
            try
            {
                lock (objectLocker)
                {
                    _logger.Trace($"关闭了一个数据库连接.");
                    conn.Close();
                    conn.Dispose();
                }
            }
            catch (Exception ex)
            {
                _logger.Error(ex, $"关闭连接异常.");
                throw ex;
            }
        }

        /// <summary>
        /// 释放资源
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
        }

        /// <summary>
        /// 释放资源
        /// </summary>
        /// <param name="isDisposing">挂起</param>
        protected virtual void Dispose(bool isDisposing)
        {
            if (isDisposing)
            {
                _mPooledconnections.ForEach(x => x.GetConnection().Close());
            }
        }
    }

    /// <summary>
    /// 数据库连接池集合
    /// </summary>
    public class ConnectionPoolList
    {
        private Hashtable _poolList;

        /// <summary>
        /// 构造函数
        /// </summary>
        public ConnectionPoolList()
        {
            _poolList = new Hashtable();
        }

        /// <summary>
        /// 创建一个连接池
        /// </summary>
        /// <param name="key"></param>
        /// <param name="option"></param>
        /// <returns></returns>
        public bool CreateConnectionPool(string key, DBOptions option)
        {
            if (!_poolList.ContainsKey(key))
            {
                _poolList.Add(key, ConnectionPool.CreateConnectionPoolInstance(option).CreatePool());
                return true;
            }
            return false;
        }

        /// <summary>
        /// 移除一个连接池
        /// </summary>
        /// <param name="key"></param>
        public void RemoveConnectionPool(string key)
        {
            if (_poolList.ContainsKey(key))
                _poolList.Remove(key);
        }

        /// <summary>
        /// 获取一个连接池
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public ConnectionPool GetConnectionPool(string key)
        {
            if (_poolList.ContainsKey(key))
                return (ConnectionPool)_poolList[key];
            else
                return null;
        }
    }

    /// <summary>
    /// 数据库连接选项
    /// </summary>
    public class DBOptions
    {
        /// <summary>
        /// 数据库连接字符串
        /// </summary>
        public string ConnectionString { get; set; }

        /// <summary>
        /// 连接池最大连接数
        /// </summary>
        public int MaxConnections { get; set; } = 50;

        /// <summary>
        /// 连接池每次增长的连接数量
        /// </summary>
        public int IncrementalConnections { get; set; } = 5;

        /// <summary>
        /// 连接池初始连接数
        /// </summary>
        public int InitialConnections { get; set; } = 10;

        /// <summary>
        /// 数据库执行语句时的超时时间(s)
        /// </summary>
        public int CommandTimeout { get; set; } = 1800;

        /// <summary>
        /// 是否应用连接池
        /// </summary>
        public bool UseConnectPool { get; set; }

        public DBOptions()
        {
        }

        public DBOptions(string connStr)
        {
            this.ConnectionString = connStr;
        }
    }

    /// <summary>
    /// 数据库访问对象
    /// </summary>
    public class MySqlDB : IDisposable
    {
        /// <summary>
        /// 数据库对象的唯一id
        /// </summary>
        public string ID { get; } = Guid.NewGuid().ToString();

        /// <summary>
        /// 数据库对象的Key，可以用来区分连接池
        /// </summary>
        public string Key { get; set; } = "Default";

        /// <summary>
        /// 数据库对象连接池，一个数据库对象拥有一个连接池
        /// </summary>
        private ConnectionPool connectionPool;

        private MySqlCommand sqlCommand;

        public static string EscapeString(string v)
        {
            if (string.IsNullOrEmpty(v)) return v;
            return MySql.Data.MySqlClient.MySqlHelper.EscapeString(v);
        }

        #region //数据库访问

        private DBOptions _mySqlDBOptions = new DBOptions();

        /// <summary>
        /// 数据库连接配置
        /// </summary>
        public DBOptions MySqlDBOptions { get => _mySqlDBOptions; set => _mySqlDBOptions = value; }

        /// <summary>
        /// 构造函数
        /// </summary>
        /// <param name="mySqlDBOptions"></param>
        public MySqlDB(DBOptions mySqlDBOptions)
        {
            _mySqlDBOptions = mySqlDBOptions;

            // 创建一个数据库连接
            if (_mySqlDBOptions.UseConnectPool && connectionPool == null)
                connectionPool = ConnectionPool.CreateConnectionPoolInstance(mySqlDBOptions).CreatePool();
        }

        /// <summary>
        /// 构造函数
        /// </summary>
        /// <param name="mySqlDBOptions"></param>
        public MySqlDB(string connStr, int timeout = 1800)
        {
            // 创建一个数据库连接
            _mySqlDBOptions = new DBOptions { ConnectionString = connStr, CommandTimeout = timeout };
        }

        /// <summary>
        /// 构造函数
        /// </summary>
        /// <param name="mySqlDBOptions"></param>
        public MySqlDB(MySqlCommand mySqlCommand)
        {
            sqlCommand = mySqlCommand;
        }

        /// <summary>
        /// 获取一个数据库连接
        /// <para>如果没有启用连接池则返回一个单连接</para>
        /// </summary>
        /// <param name="needCreate">是否强制创建</param>
        /// <returns></returns>
        private MySqlConnection GetConnection(bool needCreate = true)
        {
            try
            {
                if (!needCreate && sqlCommand != null)
                    return sqlCommand.Connection;

                if (_mySqlDBOptions.UseConnectPool && connectionPool != null)
                    return connectionPool.GetConnection();

                if (sqlCommand != null)
                    return new MySqlConnection(sqlCommand.Connection.ConnectionString);

                return new MySqlConnection(_mySqlDBOptions.ConnectionString);
            }
            catch (Exception ex)
            {
                DBLogger.Logger.Error(ex, ex.Message);
                return null;
            }
        }

        /// <summary>
        /// 释放连接
        /// <para>如果使用了连接池则用连接池释放</para>
        /// </summary>
        /// <param name="conn"></param>
        private void CloseConnection(MySqlConnection conn)
        {
            if (conn != null && conn.State != ConnectionState.Closed)
            {
                if (connectionPool != null) connectionPool.ReturnConnection(conn);
                else { conn.Close(); conn.Dispose(); }
            }
        }

        /// <summary>
        /// 执行sql语句
        /// </summary>
        /// <param name="conn">数据库连接对象</param>
        /// <param name="cmd">sqlCommand对象</param>
        /// <param name="sql">sql语句</param>
        /// <param name="param">sql参数</param>
        /// <returns></returns>
        public DBResult Execute(MySqlConnection conn, MySqlCommand cmd, string sql, params object[] param)
        {
            DBResult result = new DBResult();
            try
            {
                cmd.CommandType = CommandType.Text;
                cmd.CommandText = sql;
                cmd.CommandTimeout = _mySqlDBOptions.CommandTimeout;
                InitParameters(cmd, param);
                result.AffectRowCount = cmd.ExecuteNonQuery();//执行非查询SQL语句
                result.IsSuccessed = true;
            }
            catch (Exception ex)
            {
                result.IsSuccessed = false;
                result.Message = ex.Message;
                DBLogger.Logger.Error(ex, ex.Message + DBLogger.CallingHistory());
            }
            return result;
        }

        /// <summary>
        /// 执行sql语句
        /// </summary>
        /// <param name="sql">sql语句</param>
        /// <param name="param">sql参数</param>
        /// <returns></returns>
        public DBResult Execute(string sql, params object[] param)
        {
            DBResult result = new DBResult();
            // 执行语句，不强制创建conn对象，如果有则继承，这样才能保证事务的正常运行
            var conn = GetConnection(false);
            try
            {
                if (conn == null)
                {
                    DBLogger.Logger.Error(result.Message = "数据库连接失败..");
                    result.IsSuccessed = false;
                    return result;
                }
                if (conn.State != ConnectionState.Open)
                {
                    conn.Open();
                }

                if (sqlCommand == null)
                {
                    using (var cmd = new MySqlCommand(sql, conn))
                    {
                        result = Execute(conn, cmd, sql, param);
                    };
                }
                else
                    result = Execute(conn, sqlCommand, sql, param);
            }
            catch (Exception ex)
            {
                result.IsSuccessed = false;
                result.Message = ex.Message;
                DBLogger.Logger.Error(ex, ex.Message + DBLogger.CallingHistory());
            }
            finally
            {
                // 释放连接的时候，如果sqlCommand不为null，则为事务传递中的Command对象，在这里不做释放，否则正常释放
                if (sqlCommand == null)
                    CloseConnection(conn);
            }

            return result;
        }

        [Obsolete("可能在以后会移除此方法，请使用 ExecuteWithTransaction(Func<MySqlDB, DBResult> action) 代替")]
        /// <summary>
        /// 通过事务批量执行sql语句
        /// </summary>
        /// <param name="sql">sql语句集合</param>
        /// <returns></returns>
        public DBResult ExecuteWithTransaction(List<string> sql)
        {
            DBResult result = new DBResult();
            var conn = GetConnection();
            try
            {
                if (conn == null)
                {
                    DBLogger.Logger.Error(result.Message = "数据库连接失败..");
                    result.IsSuccessed = false;
                    return result;
                }
                if (conn.State != ConnectionState.Open)
                {
                    conn.Open();
                }
                using (MySqlCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandTimeout = _mySqlDBOptions.CommandTimeout;
                    var transaction = conn.BeginTransaction();
                    cmd.Transaction = transaction;
                    try
                    {
                        int i = 1;
                        int count = sql.Count;
                        DBLogger.Logger.Info($"本次事务共须执行{count}次.");
                        foreach (var item in sql)
                        {
                            cmd.CommandText = item;
                            result.AffectRowCount += cmd.ExecuteNonQuery();//执行非查询SQL语句
                            if (i % 500 == 0)
                                DBLogger.Logger.Info($"当前执行进度{i}/{count}次.");
                            i++;
                        }

                        DBLogger.Logger.Info($"本次事务执行完毕.");
                        transaction.Commit();
                        result.IsSuccessed = true;
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        result.IsSuccessed = false;
                        result.Message = ex.Message;
                        DBLogger.Logger.Error(ex, ex.Message + DBLogger.CallingHistory());
                    }
                }
            }
            catch (Exception e)
            {
                result.IsSuccessed = false;
                result.Message = e.Message;
            }
            finally
            {
                CloseConnection(conn);
            }

            return result;
        }

        /// <summary>
        /// 通过事务执行sql
        /// </summary>
        /// <param name="action">MySqlDB对象</param>
        /// <returns></returns>
        public DBResult ExecuteWithTransaction(Func<MySqlDB, DBResult> action)
        {
            DBResult result = new DBResult();
            var conn = GetConnection();
            try
            {
                if (conn == null)
                {
                    DBLogger.Logger.Error(result.Message = "数据库连接失败..");
                    result.IsSuccessed = false;
                    return result;
                }
                if (conn.State != ConnectionState.Open)
                {
                    conn.Open();
                }
                using (MySqlCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandTimeout = _mySqlDBOptions.CommandTimeout;
                    var transaction = conn.BeginTransaction();
                    cmd.Transaction = transaction;
                    try
                    {
                        // 创建一个单链接的数据库对象
                        result = action(new MySqlDB(cmd));
                        if (result.IsSuccessed)
                            transaction.Commit();
                        else
                            transaction.Rollback();
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        result.IsSuccessed = false;
                        result.Message = ex.Message;
                        DBLogger.Logger.Error(ex, ex.Message + DBLogger.CallingHistory());
                    }
                }
            }
            catch (Exception ex)
            {
                result.IsSuccessed = false;
                result.Message = ex.Message;
                DBLogger.Logger.Error(ex, ex.Message + DBLogger.CallingHistory());
            }
            finally
            {
                CloseConnection(conn);
            }

            return result;
        }

        /// <summary>
        /// 获取首行首列
        /// </summary>
        /// <param name="conn">数据库连接对象</param>
        /// <param name="cmd">sqlCommand对象</param>
        /// <param name="sql">sql语句</param>
        /// <param name="param">sql参数</param>
        /// <returns></returns>
        public DBResult GetTopObject(MySqlConnection conn, MySqlCommand cmd, string sql, params object[] param)
        {
            DBResult result = new DBResult();
            try
            {
                cmd.CommandType = CommandType.Text;
                cmd.CommandText = sql;
                cmd.CommandTimeout = _mySqlDBOptions.CommandTimeout;
                InitParameters(cmd, param);
                result.DataObject = cmd.ExecuteScalar();//执行非查询SQL语句
                result.IsSuccessed = true;
            }
            catch (Exception ex)
            {
                result.IsSuccessed = false;
                result.Message = ex.Message;
                DBLogger.Logger.Error(ex, ex.Message + DBLogger.CallingHistory());
            }

            return result;
        }

        /// <summary>
        /// 获取首行首列
        /// </summary>
        /// <param name="sql">sql语句模板</param>
        /// <param name="param">sql语句参数</param>
        /// <returns></returns>
        public DBResult GetTopObject(string sql, params object[] param)
        {
            DBResult result = new DBResult();
            var conn = GetConnection();
            try
            {
                if (conn == null)
                {
                    DBLogger.Logger.Error(result.Message = "数据库连接失败..");
                    result.IsSuccessed = false;
                    return result;
                }
                if (conn.State != ConnectionState.Open)
                {
                    conn.Open();
                }
                using (MySqlCommand cmd = new MySqlCommand(sql, conn))
                {
                    result = GetTopObject(conn, cmd, sql, param);
                }
            }
            catch (Exception ex)
            {
                result.IsSuccessed = false;
                result.Message = ex.Message;
                DBLogger.Logger.Error(ex, ex.Message + DBLogger.CallingHistory());
            }
            finally
            {
                CloseConnection(conn);
            }

            return result;
        }

        /// <summary>
        /// 获取DataTable
        /// </summary>
        /// <param name="sql">sql语句模板</param>
        /// <param name="param">sql语句参数</param>
        /// <returns></returns>
        public DBResult GetDataTable(string sql, params object[] param)
        {
            DBResult result = new DBResult();
            var conn = GetConnection();
            try
            {
                if (conn == null)
                {
                    DBLogger.Logger.Error(result.Message = "数据库连接失败..");
                    result.IsSuccessed = false;
                    return result;
                }
                if (conn.State != ConnectionState.Open)
                {
                    conn.Open();
                }
                using (MySqlCommand cmd = new MySqlCommand(sql, conn))
                {
                    result = GetDataSet(conn, cmd, sql, param);
                }
            }
            catch (Exception ex)
            {
                result.IsSuccessed = false;
                result.Message = ex.Message;
                DBLogger.Logger.Error(ex, ex.Message + DBLogger.CallingHistory());
            }
            finally
            {
                CloseConnection(conn);
            }

            return result;
        }

        /// <summary>
        /// 通过分页的方式获取DataTable
        /// </summary>
        /// <param name="currPage">当前页</param>
        /// <param name="pageSize">页大小</param>
        /// <param name="pageBy">分页排序时用到的字段</param>
        /// <param name="isAsc">是否时升序</param>
        /// <param name="sql">sql语句模板</param>
        /// <param name="param">sql语句的参数</param>
        /// <returns></returns>
        public DBResult GetDataTable(int currPage, int pageSize, string pageBy, bool isAsc, string sql, params object[] param)
        {
            // 不执行分页
            if (currPage == -1) return GetDataTable(sql, param);

            if (currPage <= 0)
                throw new ArgumentOutOfRangeException("页码必须是大于0的整数!");
            if (pageSize <= 0)
                throw new ArgumentOutOfRangeException("每页数量必须是大于0的整数!");

            DBResult result = new DBResult();
            result.IsAsc = isAsc;
            result.PageBy = pageBy;
            var conn = GetConnection();
            try
            {
                if (conn == null)
                {
                    DBLogger.Logger.Error(result.Message = "数据库连接失败..");
                    result.IsSuccessed = false;
                    return result;
                }
                if (conn.State != ConnectionState.Open)
                {
                    conn.Open();
                }
                using (var DataAdapter = new MySqlDataAdapter())
                {
                    string order = "";
                    if (string.IsNullOrEmpty(pageBy))
                        order = " order by 1 ";
                    else
                        order = $" order by {pageBy} {(isAsc ? "asc" : "desc")},1 ";
                    sql = $@"select SQL_CALC_FOUND_ROWS * from ({sql}) tmpSql
                             {order}
                             limit {(currPage - 1) * pageSize},{pageSize};
                            select FOUND_ROWS() as totalRecords;";
                    using (MySqlCommand cmd = new MySqlCommand(sql, conn))
                    {
                        cmd.CommandTimeout = _mySqlDBOptions.CommandTimeout;
                        InitParameters(cmd, param);
                        cmd.CommandType = CommandType.Text;
                        DataAdapter.SelectCommand = cmd;
                        var ds = new DataSet();
                        DataAdapter.Fill(ds);
                        result.Table = ds.Tables[0];
                        result.IsSuccessed = true;
                        result.PageSize = pageSize;
                        result.TotalRecords = Convert.ToInt64(ds.Tables[1].Rows[0]["totalRecords"]);
                        result.CurrentPage = currPage;
                    }
                }
            }
            catch (Exception ex)
            {
                result.IsSuccessed = false;
                result.Message = ex.Message;
                DBLogger.Logger.Error(ex, ex.Message + DBLogger.CallingHistory());
            }
            finally
            {
                CloseConnection(conn);
            }

            return result;
        }

        /// <summary>
        /// 获取DataSet
        /// </summary>
        /// <param name="conn">数据库连接对象</param>
        /// <param name="cmd">SqlCommand对象</param>
        /// <param name="sql">sql语句</param>
        /// <param name="param">sql参数</param>
        /// <returns></returns>
        public DBResult GetDataSet(MySqlConnection conn, MySqlCommand cmd, string sql, params object[] param)
        {
            DBResult result = new DBResult();
            try
            {
                using (var DataAdapter = new MySqlDataAdapter())
                {
                    cmd.CommandType = CommandType.Text;
                    cmd.CommandText = sql;
                    cmd.CommandTimeout = _mySqlDBOptions.CommandTimeout;
                    InitParameters(cmd, param);
                    DataAdapter.SelectCommand = cmd;
                    var ds = new DataSet();
                    DataAdapter.Fill(ds);
                    result.DataSet = ds;
                    if (ds.Tables.Count > 0)
                        result.Table = ds.Tables[0];
                    result.IsSuccessed = true;
                }
            }
            catch (Exception ex)
            {
                result.IsSuccessed = false;
                result.Message = ex.Message;
                DBLogger.Logger.Error(ex, ex.Message + DBLogger.CallingHistory());
            }

            return result;
        }

        /// <summary>
        /// 初始化SqlCommand的参数
        /// </summary>
        /// <param name="cmd">SqlCommand对象</param>
        /// <param name="param">参数数组</param>
        public static void InitParameters(MySqlCommand cmd, object[] param)
        {
            if (param == null || param.Length == 0)
                return;
            cmd.Parameters.Clear();
            for (int i = 0; i < param.Length; i++)
            {
                if (param[i] is KeyValuePair<string, object>)
                {
                    var p = (KeyValuePair<string, object>)param[i];
                    cmd.Parameters.AddWithValue($"@{p.Key.TrimStart('@')}", p.Value);
                }
                else
                    cmd.Parameters.AddWithValue($"@{i + 1}", param[i]);
            }
        }

        /// <summary>
        /// 释放资源
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
        }

        /// <summary>
        /// 释放资源
        /// </summary>
        /// <param name="isDisposing">挂起</param>
        protected virtual void Dispose(bool isDisposing)
        {
            if (isDisposing)
            {
                if (connectionPool != null) connectionPool.Dispose();
            }
        }

        #endregion //数据库访问
    }

    internal class DBLogger
    {
        internal static ILogger Logger
        {
            get
            {
                return LogManager.GetLogger(GetCurrentMethodFullName());
            }
        }

        private static string GetCurrentMethodFullName(int index = 2)
        {
            var st = new StackTrace(true);
            return st.FrameCount > index ?
                $"{st.GetFrame(index).GetMethod().DeclaringType}.{st.GetFrame(index).GetMethod().Name}" :
                "";
        }

        internal static List<StackFrame> GetCurrentMethodStackFrames()
        {
            var ret = new StackTrace(true).GetFrames().ToList();
            // 移除当前方法的帧
            ret.RemoveAt(0);
            return ret;
        }

        internal static string CallingHistory()
        {
            var ret = new StackTrace(true).GetFrames()?.ToList();
            if (ret == null || ret.Count == 0) return "";

            // 移除当前方法的帧
            ret.RemoveAt(0);
            ret.Reverse();
            var sb = new StringBuilder();
            sb.AppendLine();
            sb.AppendLine("调用历史");
            foreach (var item in ret)
            {
                sb.AppendLine($"调用方法: {item.GetMethod().DeclaringType}.{item.GetMethod().Name}, 行号: {item.GetFileLineNumber()}, 列号: {item.GetFileColumnNumber()}");
            }
            return sb.ToString();
        }
    }
}
