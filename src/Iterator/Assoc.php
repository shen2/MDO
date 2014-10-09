<?php
namespace MDO\Iterator;

class Assoc extends Base{
	public function current(){
		$this->_resultOffset ++;
		return $this->_result->fetch_assoc();
	}
}