<?php
namespace MDO\Iterator;

class DataObject extends Base{
	protected function _fetch(){
		$data = $this->_result->fetch_assoc();
		return new $this->_fetchArgument($data, $this->_ctorArgs[0], $this->_ctorArgs[1]);
	}
	
	public function fetchAll(){
		$rowset = new \SplFixedArray($this->_result->num_rows);
		$index = 0;
		$this->_result->data_seek(0);
		while($row = $this->_result->fetch_assoc()){
			$rowset[$index++] = new $this->_fetchArgument($row, $this->_ctorArgs[0], $this->_ctorArgs[1]);
		}
		
		return $rowset;
	}
}
