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
	
	public function __toString(){
		return $this->_select->assemble();
	}
	
	public function __call($name, $args){
		$this->_query();
		 
		return call_user_func_array(array($this->_result, $name), $args);
	}
	
	public function getResult(){
		if (!isset($this->_result)) $this->_query();
		
		return $this->_result;
	}
	
	public function setResult($result){
		$this->_result = $result;
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
		
		$this->_result->data_seek(0);
		while($row = $this->_result->fetch_assoc()){
			yield $row;
		}
	}
	
	/**
	 * @return \Generator
	 */
	public function getAssocMapGenerator(){
		if (!isset($this->_result)) $this->_query();
		
		$this->_result->data_seek(0);
		while($data = $this->_result->fetch_assoc()){
			$key = current($data);
			yield $key => $data;
		}
	}
	
	/**
	 * @param int $colno
	 * @return \Generator
	 */
	public function getColumnGenerator($colno = 0){
		if (!isset($this->_result)) $this->_query();
		
		$this->_result->data_seek(0);
		while($data = $this->_result->fetch_row()){
			yield $data[$colno];
		}
	}
	
	/**
	 * 
	 * @param string $name
	 * @return \Generator
	 */
	public function getFieldGenerator($name){
		if (!isset($this->_result)) $this->_query();
	
		$this->_result->data_seek(0);
		while($data = $this->_result->fetch_assoc()){
			yield $data[$name];
		}
	}
	
	/**
	 * @return \Generator
	 */
	public function getDataObjectGenerator($rowClass, $stored, $readOnly){
		if (!isset($this->_result)) $this->_query();
		
		$this->_result->data_seek(0);
		while($data = $this->_result->fetch_assoc()){
			yield new $rowClass($data, $stored, $readOnly);
		}
	}
	
	/**
	 * @return \Generator
	 */
	public function getFuncGenerator($func){
		if (!isset($this->_result)) $this->_query();
		
		$this->_result->data_seek(0);
		while($data = $this->_result->fetch_assoc()){
			yield $func($data);
		}
	}
	
	/**
	 * @return \Generator
	 */
	public function getKeyPairGenerator(){
		if (!isset($this->_result)) $this->_query();
		
		$this->_result->data_seek(0);
		while($data = $this->_result->fetch_row()){
			yield $data[0] => $data[1];
		}
	}
	
	/**
	 * @return \Generator
	 */
	public function getObjectGenerator($rowClass, $params){
		if (!isset($this->_result)) $this->_query();
		
		$this->_result->data_seek(0);
		while($row = $this->_result->fetch_object($rowClass, $params)){
			yield $row;
		}
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
		$this->_result->data_seek(0);
	
		while($data = $this->_result->fetch_assoc()){
			$rowset[] = $data;
		}
		return $rowset;
	}
	
	/**
	 * @return array
	 */
	public function getAssocMapArray(){
		if (!isset($this->_result)) $this->_query();
	
		$rowset = [];
		$this->_result->data_seek(0);
		while($data = $this->_result->fetch_assoc()){
			$key = current($data);
			$rowset[$key] = $data;
		}
		return $rowset;
	}
	
	/**
	 * @return array
	 */
	public function getColumnArray($colno = 0){
		if (!isset($this->_result)) $this->_query();
	
		$rowset = [];
		$index = 0;
		$this->_result->data_seek(0);
		while($data = $this->_result->fetch_row()){
			$rowset[$index++] = $data[$colno];
		}
	
		return $rowset;
	}
	
	/**
	 * @return array
	 */
	public function getFieldArray($name){
		if (!isset($this->_result)) $this->_query();
	
		$rowset = [];
		$index = 0;
		$this->_result->data_seek(0);
		while($data = $this->_result->fetch_assoc()){
			$rowset[$index++] = $data[$name];
		}
	
		return $rowset;
	}
	
	/**
	 * @return array
	 */
	public function getDataObjectArray($rowClass, $readOnly = false){
		if (!isset($this->_result)) $this->_query();
	
		$rowset = [];
		$this->_result->data_seek(0);
	
		while($data = $this->_result->fetch_assoc()){
			$rowset[] = new $rowClass($data, true, $readOnly);
		}
	
		return $rowset;
	}
	
	/**
	 * @return array
	 */
	public function getFuncArray($func){
		if (!isset($this->_result)) $this->_query();
	
		$rowset = [];
		$this->_result->data_seek(0);
		while($data = $this->_result->fetch_assoc()){
			$rowset[] = $func($data);
		}
		return $rowset;
	}
	
	/**
	 * @return array
	 */
	public function getKeyPairArray(){
		if (!isset($this->_result)) $this->_query();
	
		$rowset = [];
		$this->_result->data_seek(0);
		while($data = $this->_result->fetch_row()){
			$rowset[$data[0]] = $data[1];
		}
		return $rowset;
	}
	
	/**
	 * @return array
	 */
	public function getObjectArray($rowClass, $params){
		if (!isset($this->_result)) $this->_query();
	
		$rowset = [];
		$this->_result->data_seek(0);
		while($row = $this->_result->fetch_object($rowClass, $params)){
			$rowset[] = $row;
		}
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
