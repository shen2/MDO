<?php
namespace MDO;

class Insert extends Query{
	
	protected $_table;
	
	protected $_parts = [
		'table'		=> null,
		'keyword'	=> '',
		'columns'	=> [],
		'values'	=> [],
		'update'	=> [],
	];
	
	public function into($table){
		$this->_parts['table'] = $table;
		return $this;
	}
	
	public function columns($cols){
		$this->_parts['columns'] = [];
		foreach ($cols as $col) {
			$this->_parts['columns'][] = $this->_adapter->quoteIdentifier($col, true);
		}
		return $this;
	}
	
	protected function _renderColumns(){
		 return ' (' . implode(', ', $this->_parts['columns']) . ') ';
	}
	
	public function values($values){
		if (empty($this->_parts['values']))
			$this->_parts['values'] = [];
		
		$this->_parts['values'][] = '(' . $this->_adapter->quoteArray($values) . ')';
		
		return $this;
	}
	
	protected function _renderValues(){
		return ' VALUES ' . implode(', ', $this->_parts['values']) . ' ';
	}
	
	/**
	 * 
	 * @param string $keyword
	 */
	public function setKeyword($keyword = null){
		// 'DELAYED' 'IGNORE'
		$this->_parts['keyword'] = $keyword;
		return $this;
	}
	
	public function onDuplicateKeyUpdate(array $data)
	{
		$this->_parts['update'] = [];
		foreach ($data as $col => $val) {
			$this->_parts['update'][] = $this->_adapter->quoteIdentifier($col, true) . ' = ' . $this->_adapter->quote($val);
		}
		
		return $this;
	}
	
	public function assemble(){
		$sql = 'INSERT ' . ($this->_parts['keyword'] ?: '') . ' INTO '
			 . $this->_adapter->quoteIdentifier($this->_parts['table'], true)
			 . $this->_renderColumns()
			 . $this->_renderValues();
		
		if (!empty($this->_parts['update'])){
			$sql .= ' ON DUPLICATE KEY UPDATE ' . implode(', ', $this->_parts['update']);
		}
		
		return $sql;
	}
}
