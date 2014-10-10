<?php
namespace MDO\Iterator;

class Func extends Base{
	protected function _fetch(){
		$func = $this->_fetchArgument;
		$data = $this->_result->fetch_assoc();
		if (!$data)
			return false;
		
		return $func($data);
	}
	
	public function fetchAll(){
		$rowset = new \SplFixedArray($this->_result->num_rows);
		$index = 0;
		
		$func = $this->_fetchArgument;
		while($data = $this->_result->fetch_assoc()){
			$rowset[$index] = $func($data);
		}
		return $rowset;
	}
}