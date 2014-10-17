<?php
namespace MDO;

class Delete extends Query{
	
	protected $_parts = [
		'table'		=> null,
		self::WHERE	=> [],
		self::UNION => [],
	];
	
	/**
	 * 
	 * @param string $table
	 * @return self
	 */
	public function from($table){
		$this->_parts['table'] = $table;
		
		return $this;
	}
	
	public function assemble(){
		/**
		 * Build the DELETE statement
		 */
		$sql = "DELETE FROM "
				. $this->_adapter->quoteIdentifier($this->_parts['table'], true);
		
		if ($this->_parts['where'])
			$sql .= $this->_renderWhere();
		else	// FIXME temp, for warning.
			throw new Exception('Warning: where clauses is empty.');
		
		return $sql;
	}
}
