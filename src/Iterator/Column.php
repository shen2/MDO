<?php
namespace MDO\Iterator;

class Column extends Base{
	protected function _fetch(){
		return $this->_result->fetch_column(0);
	}
	
	public function fetchAll(){
		$rowset = new \SplFixedArray($this->_result->num_rows);
		$index = 0;
		while($row = $this->_result->fetch_assoc()){
			$rowset[$index++] = $row[0];
		}
		return $rowset;
	}
}
