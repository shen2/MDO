<?php
namespace MDO\Iterator;

class Column extends Base{
	public function current(){
		$this->_resultOffset ++;
		return $this->_result->fetch_column(0);
	}
}