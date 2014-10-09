<?php
namespace MDO\Iterator;

class ClassFunc extends Base{
	protected function _fetch(){
		$classFunc = $this->_fetchArgument;
		$data = $this->_result->fetch_assoc();
		if (!$data)
			return false;
		
		$rowClass = $classFunc($data);
		return new $rowClass($data, true, $this->_ctorArgs);
	}
	
	public function fetchAll(){
		$rowset = new \SplFixedArray($this->_result->num_rows);
		$index = 0;
		
		$classFunc = $this->_fetchArgument;
		while($data = $this->_result->fetch_assoc()){
			$rowClass = $classFunc($data);
			$rowset[$index] = new $rowClass($data, true, $this->_ctorArgs);
		}
		return $rowset;
	}
}