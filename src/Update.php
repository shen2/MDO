<?php
namespace MDO;

class Update extends Query{
	
	protected $_parts = [
		'table'	=> null,
		'set'	=> [],
		self::WHERE	=> [],
		self::UNION => [],
	];
	
	public function table($table){
		$this->_parts['table'] = $table;
		return $this;
	}
	
	public function set($data){
		/**
		 * Build "col = ?" pairs for the statement,
		 * except for Expr which is treated literally.
		 */
		foreach ($data as $col => $val) {
			$this->_parts['set'][] = $this->_adapter->quoteIdentifier($col, true) . ' = ' . $this->_adapter->quote($val);
		}
		
		return $this;
	}
	
	public function assemble(){
		/**
		 * Build the UPDATE statement
		 */
		$sql = "UPDATE "
				. $this->_adapter->quoteIdentifier($this->_parts['table'], true)
				. ' SET ' . implode(', ', $this->_parts['set']);
		
		if ($this->_parts['where'])
			$sql .= $this->_renderWhere();
		else	// FIXME temp, for warning.
			throw new Exception('Warning: where clauses is empty.');
		
		return $sql;
	}
}
