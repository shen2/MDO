<?php
namespace MDO;

class Statement implements \IteratorAggregate, \Countable
{
	/**
	 * 
	 * @var Adapter
	 */
	protected $_connection = null;
	
	/**
	 * 
	 * @var Select
	 */
	protected $_select;
	
	/**
	 * 
	 * @var Iterator\Base
	 */
	protected $_iterator;
	
	protected $_result = null;
	
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
	}
	
	/**
	 * 
	 * @param Iterator\Base $iterator
	 */
	public function setIterator($iterator){
		$this->_iterator = $iterator;
		
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
	
	/**
	 * 
	 * @return \Iterator
	 */
	public function getIterator(){
		if (!isset($this->_result)) $this->_query();
		
		$this->_iterator->setResult($this->_result);
		$all = $this->_iterator->fetchAll();
		
		if (is_array($all))
			return new \ArrayIterator($all);
		
		return $all;
	}
	
	public function assemble(){
		return $this->_select->assemble();
	}
	
	/**
	 * 
	 * @throws \mysqli_sql_exception
	 */
	public function _query(){
		//如果已经在结果缓存中，则搜寻结果集
		$this->_connection->flushQueue($this);
		
		if (isset($this->_result))
			return;
		
		$this->_connection->queryStatement($this);
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
