<?php
namespace MDO\Iterator;

class AssocMap extends Base{
	/**
	 * 
	 * @var array
	 */
	protected $_data;
	
	protected function _fetch(){
		$this->_data = $this->_result->fetch_array(\MYSQLI_ASSOC);
	}
	
	public function current(){
		if (empty($this->_data))
			$this->_fetch();
		
		return $this->_data;
	}
	
	public function key(){
		if (empty($this->_data))
			$this->_fetch();
		
		return current($this->_data);
	}
	
	public function next(){
		parent::next();
		
		$this->_data = null;
	}
	
	public function fetchAll(){
		$map = array();
		while($data = $this->_result->fetch_array(\MYSQLI_ASSOC)){
			$map[current($data)] = $data;
		}
		
		return $map;
	}
}
