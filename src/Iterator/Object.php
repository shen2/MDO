<?php
namespace MDO\Iterator;

class Object extends Base{
	protected function _fetch(){
		return $this->_result->fetch_object($this->_fetchArgument, $this->_ctorArgs);
	}
	
	public function fetchAll(){
		$rowset = new \SplFixedArray($this->_result->num_rows);
		$index = 0;
		while($row = $this->_result->fetch_object($this->_fetchArgument, $this->_ctorArgs)){
			$rowset[$index++] = $row;
		}
		
		return $rowset;
	}
}