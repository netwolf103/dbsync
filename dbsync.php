<?php
set_time_limit(0);

/**
 * @file
 *
 * 数据库同步PHP脚本
 **/

// 远程服务器
define( 'REMOTE_DB_HOST', '' );
define( 'REMOTE_DB_USER', '' );
define( 'REMOTE_DB_PASS', '' );
define( 'REMOTE_DB_NAME', '' );

// 本地服务器
define( 'LOCAL_DB_HOST', 'localhost:3306' );
define( 'LOCAL_DB_USER', 'root' );
define( 'LOCAL_DB_PASS', '111111' );
define( 'LOCAL_DB_NAME', 'test' );

// 日志路径
define( 'LOG_PATH', 'logs/test/' );

// 开启调试
define( 'DEBUG', true );

if( DEBUG ) {
	error_reporting( E_ERROR | E_WARNING | E_PARSE | E_NOTICE );
} else {
	error_reporting(0);
}

class ygdb
{
	/**
	 * MySQL连接标识
	 *
	 * @access private
	 */
	var $_link	= false;

	/**
	 * MySQL查询结果
	 *
	 * @access private
	 */
	var $result = array();

	/**
	 * 数据库所有表名
	 *
	 * @access private
	 */
	var $_tables = array();

	/**
	 * 数据表主键
	 *
	 * @access public
	 */
	var $primary_keys = array();
	
	/**
	 * MySQL构造方法
	 *
	 * @access public
	 * @param string, $host
	 * @param string, $dbname
	 * @param string, $dbpass
	 * @param string, $db
	 */
	function __construct( $host, $dbname, $dbpass, $db = '' )
	{
		register_shutdown_function( array( &$this, '__destruct' ) );

		$this->_link = mysql_connect( $host, $dbname, $dbpass);

		if( !$this->_link ) {
			die( 'Could not connect: ' . mysql_error() );
		}

		$this->query('SET NAMES UTF8');

		if( !empty($db) ) {
			mysql_select_db( $db, $this->_link) or die('Could not use ' . $db . ': ' . mysql_error() );

			$result = @mysql_query( "SHOW TABLES FROM {$db}" );
			while ( $row = mysql_fetch_array( $result, MYSQL_NUM ) ) {

				$this->_tables[] = $row[0];
			}

			foreach( $this->_tables as $table ) {
				
				$result = @mysql_query( "SELECT * FROM {$table} LIMIT 1" );

				if( !$result ) continue;

				$i = 0;
				while ($i < mysql_num_fields($result)) {
					$meta = mysql_fetch_field($result);
					
					if( $meta->primary_key == 1 ) {
						$this->primary_keys[$table] = $meta->name;
						break;
					}
					$i++;
				}

			}

		}
	}
	
	/**
	 * MySQL面向对象兼容方法
	 *
	 * @access public
	 * @param string, $host
	 * @param string, $dbname
	 * @param string, $dbpass
	 * @param string, $db
	 */
	function mysql( $host, $dbname, $dbpass, $db )
	{
		$this->_construct( $host, $dbname, $dbpass, $db );
	}

	/**
	 * Insert data
	 *
	 * @access public
	 * @param string, $table
	 * @param array, $data
	 * @return intval
	 */
	function insert( $table, $data )
	{
		$sql = "INSERT INTO `{$table}` SET ";

		foreach($data as $k=>$v) {
			$sql .= "`{$k}`='{$v}',";
		}
		$sql = substr($sql, 0, -1);

		return $this->query($sql);
	}

	/**
	 * Update data
	 *
	 * @access public
	 * @param string, $table
	 * @param array, $data
	 * @return intval
	 */
	function update( $table, $data, $where )
	{
		$sql = "UPDATE `{$table}` SET ";

		foreach($data as $k=>$v) {
			$sql .= "`{$k}`='{$v}',";
		}
		$sql = substr($sql, 0, -1);

		$sql .= "WHERE {$where}";

		return $this->query($sql);
	}
	
	/**
	 * 执行SQL查询
	 *
	 * @access public
	 * @param string, $sql
	 * @return intval
	 */
	function query( $sql )
	{
		$return_val = 0;

		$query = @mysql_query( $sql, $this->_link );

		if ( preg_match( '/^\s*(insert|delete|update|replace) /i', $sql ) ) {
			$return_val = mysql_affected_rows( $this->_link );
		} else {
			$num_rows = 0;
			while ( $row = @mysql_fetch_object( $query ) ) {
				$this->result[$num_rows] = $row;
				$num_rows++;
			}
			
			$return_val = $num_rows;
			@mysql_free_result( $query );
		}

		return $return_val;
	}

	/**
	 * 返回结果集中一行记录
	 *
	 * @access public
	 * @param string, SQL
	 * @return array
	 */
	function get_results( $sql )
	{
		$return = array();

		if( $this->query( $sql ) ) {
			$return = $this->result;
			$this->result = array();
		}

		return $return;
	}

	/**
	 * 返回结果集中一行记录
	 *
	 * @access public
	 * @param string, SQL
	 * @return object
	 */
	function get_row( $sql )
	{
		if( $this->query( $sql ) ) {
			return $this->result[0];
		}

		return false;
	}

	/**
	 * 返回最后一次INSERT ID
	 */
	function last_insert_id()
	{
		return mysql_insert_id($this->_link);
	}

	/**
	 * 获取数据表字段名
	 */
	function fields( $table )
	{
		$fields = array();

		$columns = $this->get_results( "SHOW FULL COLUMNS FROM {$table}" );

		foreach( $columns as $column ) {
			$fields[] = $column->Field;
		}

		return $fields;
	}
	
	/**
	 * 析构函数
	 */
	function __destruct()
	{
		return true;
	}
}

/**
 * 数据表同步
 **/
function sync( $table, $primary_key )
{
	global $local_db, $remote_db, $handle;

	$local_max = $local_db->get_row( 'SELECT MAX(' .$primary_key. ') AS max FROM ' . $table );
	$remote_max = $remote_db->get_row( 'SELECT MAX(' .$primary_key. ') AS max FROM ' . $table );

	if( (int)$local_max->max < (int)$remote_max->max ) {
		
		$success_total = $failure_total = 0;

		// 写入日志开始
		@fwrite( $handle, "sync: {$table}\r\n" );
		@fwrite( $handle, "\tID\tStatus\r\n" );

		$results = $remote_db->get_results( 'SELECT * FROM ' .$table. ' WHERE ' .$primary_key. ' > ' . (int)$local_max->max . ' ORDER BY ' .$primary_key. ' ASC' );

		foreach( $results as $result ) {
			
			if( $local_db->insert( $table, (array)$result ) ) {
				@fwrite( $handle, "\t" .$result->{$primary_key}. "\tSuccess\r\n" );
				$success_total++;
			} else {
				@fwrite( $handle, "\t" .$result->{$primary_key}. "}\tFailure\r\n" );
				$failure_total++;
			}
		}
		@fwrite( $handle, "Successful total: {$success_total}\r\n" );
		@fwrite( $handle, "Failure total: {$failure_total}\r\n\r\n" );
	}
}

// 日志存放路径
if( !is_dir( LOG_PATH ) ) {
	if( !mkdir( LOG_PATH, 0777, true ) ) {
		die( '无法创建日志目录：' . LOG_PATH . ', 请手工创建' );
	}
}

// 创建日志文件
$filename = LOG_PATH . date('Ymd-His') . '.log';

if( !$handle = fopen($filename, 'a') ) {
	die("无法打开文件 {$filename}, 请手工创建");
}

// 远程服务器
$remote_db = new ygdb( REMOTE_DB_HOST, REMOTE_DB_USER, REMOTE_DB_PASS, REMOTE_DB_NAME );
$remote_tables = $remote_db->_tables;

// 本地服务器
$local_db = new ygdb( LOCAL_DB_HOST, LOCAL_DB_USER, LOCAL_DB_PASS, LOCAL_DB_NAME );
$local_tables = $local_db->_tables;

// 同步表结构
foreach( array_diff($remote_tables, $local_tables) as $table ) {
	$temp = $remote_db->get_row( 'SHOW CREATE TABLE ' . $table );
	$temp = (array) $temp;

	$local_db->query( $temp['Create Table'] );
}

// 同步表数据
foreach( $local_db->primary_keys as $table => $primary_key ) {
	sync( $table, $primary_key );
}

?>