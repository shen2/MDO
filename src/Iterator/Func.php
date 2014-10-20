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
		$rowset = new $this->_rowsetClass($this->_result->num_rows);
		$index = 0;
		$this->_result->data_seek(0);
		$func = $this->_fetchArgument;
		while($data = $this->_result->fetch_assoc()){
			$rowset[$index] = $func($data);
		}
		return $rowset;
	}
}
