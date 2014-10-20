<?php
namespace MDO\Iterator;

class Assoc extends Base{
	protected function _fetch(){
		return $this->_result->fetch_assoc();
	}
	
	public function fetchAll(){
		$rowset = new \SplFixedArray($this->_result->num_rows);
		$index = 0;
		$this->_result->data_seek(0);
		while($row = $this->_result->fetch_assoc()){
			$rowset[$index++] = $row;
		}
		
		return $rowset;
	}
}
