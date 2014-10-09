<?php
namespace MDO;

class Statement implements \IteratorAggregate, \Countable
{
	/* copyed from PDO */
	const FETCH_LAZY = 1;
	const FETCH_ASSOC = 2;
	const FETCH_NUM = 3;
	const FETCH_BOTH = 4;
	const FETCH_OBJ = 5;
	const FETCH_BOUND = 6;
	const FETCH_COLUMN = 7;
	const FETCH_CLASS = 8;
	const FETCH_INTO = 9;
	const FETCH_FUNC = 10;
	const FETCH_NAMED = 11;
	const FETCH_KEY_PAIR = 12;
	
	const FETCH_GROUP = 65536;
	const FETCH_UNIQUE = 196608;
	const FETCH_CLASSTYPE = 262144;
	const FETCH_SERIALIZE = 524288;
	const FETCH_PROPS_LATE = 1048576;
	
	const FETCH_DATAOBJECT = 'fetchDataObject';
	const FETCH_CLASSFUNC = 'fetchClassFunc';
	
	/**
	 * 
	 * @var \mysqli
	 */
	protected $_connection = null;
	
	/**
	 * 
	 * @var Select
	 */
	protected $_select;
	
	protected $_fetchMode;
	
	protected $_result = false;
	
	/**
	 * 构造函数
	 * 
	 * @param $connection
	 * @param $select
	 * @param $fetchMode
	 * @param $fetchArgument
	 * @param $ctorArgs
	 */
	public function __construct($connection, $select){
		$this->_connection = $connection;
		$this->_select = $select;
		
		self::$_waitingQueue[] = $this;
	}
	
	/**
	 * 
	 * @param int $fetchMode
	 * @param mixed $fetchArgument
	 * @param array $ctorArgs
	 */
	public function setFetchMode($fetchMode, $fetchArgument = null, $ctorArgs = null){
		$this->_fetchMode = $fetchMode;
		
		return $this;
	}
	
	public function __toString(){
		return $this->_select->assemble();
	}
	
	public function __call($name, $args){
		$this->_query();
		 
		return call_user_func_array(array($this->_result, $name), $args);
	}
	
	public function getResult(){
		return $this->_result;
	}
	
	public function setResult($result){
		$this->_result = $result;
		return $this;
	}
	
	protected function _fetchAll(){
		switch ($this->_fetchMode){
			case self::FETCH_ASSOC:
				$rowset = new \SplFixedArray($this->_result->num_rows);
				$index = 0;
				while($row = $this->_result->fetch_assoc()){
					$rowset[$index++] = $row;
				}
			
				return $rowset;
				
			case self::FETCH_DATAOBJECT:
				$rowset = new \SplFixedArray($this->_result->num_rows);
				$index = 0;
				while($row = $this->_result->fetch_object($this->_fetchArgument, [true, $this->_ctorArgs])){
					$rowset[$index++] = $row;
				}
				
				return $rowset;
			
			case self::FETCH_CLASSFUNC:
				$rowset = new \SplFixedArray($this->_result->num_rows);
				$index = 0;
				
				$classFunc = $this->_fetchArgument;
				while($data = $this->_result->fetch_assoc()){
					$rowClass = $classFunc($data);
					$rowset[$index] = new $rowClass($data, true, $this->_ctorArgs);
				}
				return $rowset;

			case self::FETCH_COLUMN:
				$rowset = new \SplFixedArray($this->_result->num_rows);
				$index = 0;
				while($row = $this->_result->fetch_assoc()){
					$rowset[$index++] = $row[0];
				}
				return $rowset;
				
			case self::FETCH_KEY_PAIR:
				$map = array();
				while($data = $this->_result->fetch_assoc()){
					$map[$data[0]] = $data[1];
				}
				
				return $map;
			
			default:
				if (isset($this->_ctorArgs))
					return self::$_stmt->fetchAll($this->_fetchMode, $this->_fetchArgument, $this->_ctorArgs);
				
				if (isset($this->_fetchArgument))
					return self::$_stmt->fetchAll($this->_fetchMode, $this->_fetchArgument);
					
				if (isset($this->_fetchMode))
					return self::$_stmt->fetchAll($this->_fetchMode);
		}
	}
	
	/**
	 * 
	 * @throws \mysqli_sql_exception
	 */
	public function _query(){
		//如果已经在结果缓存中，则搜寻结果集
		$this->_connection->flushQueue($statement);
		
		if (isset($this->_result))
			return;
		
		$this->_connection->waitingUntilStatement();
	}
	
	/**
	 * 强制获得结果集
	 * 
	 * @return mixed
	 */
	public function fetch(){
		if (!isset($this->_result)) $this->_query();
		
		return $this->_result;
	}
	
	/**
	 * 获得迭代器，支持foreach
	 */
	public function getIterator(){
		if (!isset($this->_result)) $this->_query();
		
		if (is_array($this->_result))
			return new \ArrayIterator($this->_result);
		
		return $this->_result;
	}
	
	public function current(){
		if (!isset($this->_result)) $this->_query();
		
		if ($this->_result instanceof \SplFixedArray){
			// php 5.4.6中有一个bug, 在小概率(5%)情况下，SplFixedArray的count不为0，但是current()取不到实际的数据，var_dump()时也显示SplFixedArray(0)，必须用[0]才能取到数据。
			return $this->_result->num_rows
				? ($this->_result->current() ?: $this->_result[0])
				: null;
		}
		else{//普通数组或者其他实现了count和current方法的对象
			return count($this->_result) ? current($this->_result) : null;
		}
	}
	
	public function count() {
		if (!isset($this->_result)) $this->_query();
		
		return $this->_result->num_rows;
	}
	
	/**
	 * 用来代替current
	 * 
	 * @return mixed
	 */
	public function first(){
		if (!isset($this->_result)) $this->_query();
		 
		return isset($this->_result[0]) ? $this->_result[0] : null;
	}
}
