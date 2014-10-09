<?php
namespace MDO\Iterator;

class DataObject extends Base{
	public function current(){
		$this->_resultOffset ++;
		return $this->_result->fetch_object($this->_fetchArgument, [true, $this->_ctorArgs]);
	}
}