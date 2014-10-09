<?php
namespace MDO\Iterator;

class ClassFunc extends Base{
	public function current(){
		$this->_resultOffset ++;
		
		$classFunc = $this->_fetchArgument;
		$data = $this->_result->fetch_assoc();
		if (!$data)
			return false;
		
		$rowClass = $classFunc($data);
		return new $rowClass($data, true, $this->_ctorArgs);
	}
}