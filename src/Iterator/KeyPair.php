<?php
namespace MDO\Iterator;

class KeyPair extends Base{
	/**
	 * 
	 * @var string
	 */
	protected $_key;
	
	protected function _fetch(){
		$this->_resultOffset ++;
		$this->_data = $this->_result->fetch_array();
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
}