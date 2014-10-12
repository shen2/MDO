<?php
namespace MDO\Iterator;

class KeyPair extends Base{
	/**
	 * 
	 * @var array
	 */
	protected $_data;
	
	protected function _fetch(){
		$this->_data = $this->_result->fetch_array(\MYSQLI_NUM);
	}
	
	public function current(){
		if (empty($this->_data))
			$this->_fetch();
		
		return $this->_data[1];
	}
	
	public function key(){
		if (empty($this->_data))
			$this->_fetch();
		
		return $this->_data[0];
	}
	
	public function next(){
		parent::next();
		
		$this->_data = null;
	}
	
	public function fetchAll(){
		$map = array();
		while($data = $this->_result->fetch_array(\MYSQLI_NUM)){
			$map[$data[0]] = $data[1];
		}
		
		return $map;
	}
}