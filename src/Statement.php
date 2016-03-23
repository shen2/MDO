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
	
	protected $_result = null;
	
	/**
	 * 
	 * @var bool
	 */
	protected $_storeResult;
	
	/**
	 * Success Callbacks
	 * @var array
	 */
	protected $_successCallbacks = [];
	
	/**
	 * 构造函数
	 * 
	 * @param $connection
	 * @param $select
	 * @param $storeResult
	 */
	public function __construct($connection, $select, $storeResult = true){
		$this->_connection = $connection;
		$this->_select = $select;
		$this->_storeResult = $storeResult;
	}
	
	public function __toString(){
		return $this->_select->assemble();
	}
	
	public function getResult(){
		if (!isset($this->_result)) $this->_query();
		
		return $this->_result;
	}

	/**
	 *
	 * @param callable $callback
	 * @return $this
	 */
	public function onSuccess($callback){
		$this->_successCallbacks[] = $callback;
		return $this;
	}
	
	/**
	 * 
	 * @param Adapter $db
	 * @return \MDO\Statement
	 */
	public function storeResult($db){
		$this->_result = $this->_storeResult ? $db->store_result() : $db->use_result();
		
		foreach($this->_successCallbacks as $callback)
			$callback($this);
		
		return $this;
	}
	
	/**
	 * @return \Generator
	 */
	public function getIterator(){
		return $this->getAssocGenerator();
	}
	
	/**
	 * @return \Generator
	 */
	public function getAssocGenerator(){
		if (!isset($this->_result)) $this->_query();
		
		if ($this->_storeResult) $this->_result->data_seek(0);
		while($row = $this->_result->fetch_assoc()){
			yield $row;
		}
		if (!$this->_storeResult) $this->_result->free();
	}
	
	/**
	 * @param int $colno
	 * @return \Generator
	 */
	public function getColumnGenerator($colno = 0){
		if (!isset($this->_result)) $this->_query();
		
		if ($this->_storeResult) $this->_result->data_seek(0);
		while($data = $this->_result->fetch_row()){
			yield $data[$colno];
		}
		if (!$this->_storeResult) $this->_result->free();
	}
	
	/**
	 * 
	 * @param string $name
	 * @return \Generator
	 */
	public function getFieldGenerator($name){
		if (!isset($this->_result)) $this->_query();
	
		if ($this->_storeResult) $this->_result->data_seek(0);
		while($data = $this->_result->fetch_assoc()){
			yield $data[$name];
		}
		if (!$this->_storeResult) $this->_result->free();
	}
	
	/**
	 * @param $table Table
	 * @return \Generator
	 */
	public function getDataObjectGenerator($table, $readOnly = false){
		if (!isset($this->_result)) $this->_query();
		
		if ($this->_storeResult) $this->_result->data_seek(0);
		$rowClass = $table->getRowClass();
		while($data = $this->_result->fetch_assoc()){
			yield (new $rowClass($data, true, $readOnly))->setTable($table);
		}
		if (!$this->_storeResult) $this->_result->free();
	}
	
	/**
	 * @return \Generator
	 */
	public function getFuncGenerator($func){
		if (!isset($this->_result)) $this->_query();
		
		if ($this->_storeResult) $this->_result->data_seek(0);
		while($data = $this->_result->fetch_assoc()){
			yield $func($data);
		}
		if (!$this->_storeResult) $this->_result->free();
	}
	
	/**
	 * @return \Generator
	 */
	public function getKeyPairGenerator(){
		if (!isset($this->_result)) $this->_query();
		
		if ($this->_storeResult) $this->_result->data_seek(0);
		while($data = $this->_result->fetch_row()){
			yield $data[0] => $data[1];
		}
		if (!$this->_storeResult) $this->_result->free();
	}
	
	/**
	 * @return \Generator
	 */
	public function getObjectGenerator($rowClass, $params){
		if (!isset($this->_result)) $this->_query();
		
		if ($this->_storeResult) $this->_result->data_seek(0);
		while($row = $this->_result->fetch_object($rowClass, $params)){
			yield $row;
		}
		if (!$this->_storeResult) $this->_result->free();
	}
	
	public function assemble(){
		return $this->_select->assemble();
	}
	
	/**
	 * 
	 * @throws \mysqli_sql_exception
	 */
	public function _query(){
		$this->_connection->ensureConnected();
		
		//如果已经在结果缓存中，则搜寻结果集
		$this->_connection->flushQueue($this);
		
		if (isset($this->_result))
			return;
		
		$this->_connection->queryStatement($this);
	}
	
	/**
	 * 
	 * @return array
	 */
	public function getAssocArray(){
		if (!isset($this->_result)) $this->_query();
	
		$rowset = [];
		if ($this->_storeResult) $this->_result->data_seek(0);
	
		while($data = $this->_result->fetch_assoc()){
			$rowset[] = $data;
		}
		if (!$this->_storeResult) $this->_result->free();
		return $rowset;
	}
	
	/**
	 * @return array
	 */
	public function getAssocMapArray(){
		if (!isset($this->_result)) $this->_query();
	
		$rowset = [];
		if ($this->_storeResult) $this->_result->data_seek(0);
		while($data = $this->_result->fetch_assoc()){
			$key = current($data);
			$rowset[$key] = $data;
		}
		if (!$this->_storeResult) $this->_result->free();
		return $rowset;
	}
	
	/**
	 * @return array
	 */
	public function getColumnArray($colno = 0){
		if (!isset($this->_result)) $this->_query();
	
		$rowset = [];
		$index = 0;
		if ($this->_storeResult) $this->_result->data_seek(0);
		while($data = $this->_result->fetch_row()){
			$rowset[$index++] = $data[$colno];
		}
		if (!$this->_storeResult) $this->_result->free();
		return $rowset;
	}
	
	/**
	 * @return array
	 */
	public function getFieldArray($name){
		if (!isset($this->_result)) $this->_query();
	
		$rowset = [];
		$index = 0;
		if ($this->_storeResult) $this->_result->data_seek(0);
		while($data = $this->_result->fetch_assoc()){
			$rowset[$index++] = $data[$name];
		}
		if (!$this->_storeResult) $this->_result->free();
		return $rowset;
	}
	
	/**
	 * @return array
	 */
	public function getDataObjectArray($table, $readOnly = false){
		if (!isset($this->_result)) $this->_query();
	
		$rowset = [];
		$rowClass = $table->getRowClass();
		if ($this->_storeResult) $this->_result->data_seek(0);
	
		while($data = $this->_result->fetch_assoc()){
			 $rowset[] = (new $rowClass($data, true, $readOnly))->setTable($table);
		}
		if (!$this->_storeResult) $this->_result->free();
		return $rowset;
	}
	
	/**
	 * @return array
	 */
	public function getFuncArray($func){
		if (!isset($this->_result)) $this->_query();
	
		$rowset = [];
		if ($this->_storeResult) $this->_result->data_seek(0);
		while($data = $this->_result->fetch_assoc()){
			$rowset[] = $func($data);
		}
		if (!$this->_storeResult) $this->_result->free();
		return $rowset;
	}
	
	/**
	 * @return array
	 */
	public function getKeyPairArray(){
		if (!isset($this->_result)) $this->_query();
	
		$rowset = [];
		if ($this->_storeResult) $this->_result->data_seek(0);
		while($data = $this->_result->fetch_row()){
			$rowset[$data[0]] = $data[1];
		}
		if (!$this->_storeResult) $this->_result->free();
		return $rowset;
	}
	
	/**
	 * @return array
	 */
	public function getObjectArray($rowClass, $params){
		if (!isset($this->_result)) $this->_query();
	
		$rowset = [];
		if ($this->_storeResult) $this->_result->data_seek(0);
		while($row = $this->_result->fetch_object($rowClass, $params)){
			$rowset[] = $row;
		}
		if (!$this->_storeResult) $this->_result->free();
		return $rowset;
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
