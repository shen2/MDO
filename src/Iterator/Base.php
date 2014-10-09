<?php
namespace MDO\Iterator;

class Base implements \Iterator{
	
	protected $_result;
	
	protected $_fetchArgument;
	
	protected $_ctorArgs;
	
	protected $_offset = 0;
	
	protected $_resultOffset = 0;
	
	protected $_overahead = false;
	
	public function __construct($fetchArgument = null, $ctorArgs = null){
		$this->_fetchArgument = $fetchArgument;
		$this->_ctorArgs = $ctorArgs;
	}
	
	public function setResult($result){
		$this->_result = $result;
	}
	
	abstract public function current();
	
	public function next(){
		$this->_offset++;
		
		if ($this->_offset !== $this->_resultOffset){
			$this->_result->data_seek($this->_offset);
		}
	}

	public function key(){
		return $this->_offset;
	}

	public function valid () {
		return $this->_offset < $this->_result->num_rows;
	}

	public function rewind () {
		$this->_offset = 0;
		$this->_resultOffset = 0;
		$this->_result->data_seek(0);
	}
}
