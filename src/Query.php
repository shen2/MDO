<?php 
namespace MDO;

class Query {

	/**
	 * Adapter object.
	 *
	 * @var Adapter
	 */
	protected $_adapter;
	
	public function __construct(Adapter $adapter){
		$this->_adapter = $adapter;
	}
	
	/**
	 * Inserts a table row with specified data.
	 *
	 * @param mixed $table The table to insert data into.
	 * @param array $bind Column-value pairs.
	 * @return int The number of affected rows.
	 */
	public function insert($table, array $bind, $keyword = null)
	{
		// extract and quote col names from the array keys
		$cols = array();
		$vals = array();
		
		foreach ($bind as $col => $val) {
			$cols[] = $this->_adapter->quoteIdentifier($col, true);
			if ($val instanceof Expr) {
				$vals[] = $val->__toString();
				unset($bind[$col]);
			} else {
				$vals[] = '?';
			}
		}

		// build the statement
		$sql = ($keyword ? "INSERT $keyword INTO " : "INSERT INTO ")
			 . $this->_adapter->quoteIdentifier($table, true)
			 . ' (' . implode(', ', $cols) . ') '
			 . 'VALUES (' . implode(', ', $vals) . ')';

		// execute the statement and return the number of affected rows
		$bind = array_values($bind);
		$stmt = $this->query($sql, $bind);
		$result = $stmt->rowCount();
		return $result;
	}
	
	/**
	 * 使用insert delayed插入一条记录
	 *
	 * @param mixed $table The table to insert data into.
	 * @param array $bind Column-value pairs.
	 * @return int The number of affected rows.
	 */
	public function insertDelayed($table, array $bind)
	{
		return $this->insert($table, $bind, 'DELAYED');
	}
	
	/**
	 * 使用insert ignore插入一条记录
	 *
	 * @param mixed $table The table to insert data into.
	 * @param array $bind Column-value pairs.
	 * @return int The number of affected rows.
	 */
	public function insertIgnore($table, array $bind)
	{
		return $this->insert($table, $bind, 'IGNORE');
	}
	
	public function insertOnDuplicateKeyUpdate($table, array $insertBind, array $updateBind)
	{
		// extract and quote col names from the array keys
		$cols = array();
		$vals = array();
		foreach ($insertBind as $col => $val) {
			$cols[] = $this->_adapter->quoteIdentifier($col, true);
			if ($val instanceof Expr) {
				$vals[] = $val->__toString();
				unset($insertBind[$col]);
			} else {
				$vals[] = '?';
			}
		}

		/**
		 * Build "col = ?" pairs for the statement,
		 * except for Expr which is treated literally.
		 */
		$set = array();
		foreach ($updateBind as $col => $val) {
			if ($val instanceof Expr) {
				$val = $val->__toString();
				unset($updateBind[$col]);
			} else {
				$val = '?';
			}
			$set[] = $this->_adapter->quoteIdentifier($col, true) . ' = ' . $val;
		}

		// build the statement
		$sql = "INSERT INTO "
			 . $this->_adapter->quoteIdentifier($table, true)
			 . ' (' . implode(', ', $cols) . ') '
			 . 'VALUES (' . implode(', ', $vals) . ')'
			 . ' ON DUPLICATE KEY UPDATE ' . implode(', ', $set);
		
		// execute the statement and return the number of affected rows
		$bind = array_merge(array_values($insertBind), array_values($updateBind));
		$stmt = $this->query($sql, $bind);
		$result = $stmt->rowCount();
		return $result;
	}
	
	/**
	 * Updates table rows with specified data based on a WHERE clause.
	 *
	 * @param  mixed		$table The table to update.
	 * @param  array		$bind  Column-value pairs.
	 * @param  mixed		$where UPDATE WHERE clause(s).
	 * @return int		  The number of affected rows.
	 */
	public function update($table, array $bind, $where = '')
	{
		/**
		 * Build "col = ?" pairs for the statement,
		 * except for Expr which is treated literally.
		 */
		$set = array();
		foreach ($bind as $col => $val) {
			if ($val instanceof Expr) {
				$val = $val->__toString();
				unset($bind[$col]);
			} else {
				$val = '?';
			}
			$set[] = $this->_adapter->quoteIdentifier($col, true) . ' = ' . $val;
		}

		$where = $this->_whereExpr($where);

		/**
		 * Build the UPDATE statement
		 */
		$sql = "UPDATE "
			 . $this->_adapter->quoteIdentifier($table, true)
			 . ' SET ' . implode(', ', $set)
			 . (($where) ? " WHERE $where" : '');

		/**
		 * Execute the statement and return the number of affected rows
		 */
		$stmt = $this->query($sql, array_values($bind));
		$result = $stmt->rowCount();
		return $result;
	}

	/**
	 * Deletes table rows based on a WHERE clause.
	 *
	 * @param  mixed		$table The table to update.
	 * @param  mixed		$where DELETE WHERE clause(s).
	 * @return int		  The number of affected rows.
	 */
	public function delete($table, $where = '')
	{
		$where = $this->_whereExpr($where);

		/**
		 * Build the DELETE statement
		 */
		$sql = "DELETE FROM "
			 . $this->_adapter->quoteIdentifier($table, true)
			 . (($where) ? " WHERE $where" : '');

		/**
		 * Execute the statement and return the number of affected rows
		 */
		$stmt = $this->query($sql);
		$result = $stmt->rowCount();
		return $result;
	}

	/**
	 * Convert an array, string, or Expr object
	 * into a string to put in a WHERE clause.
	 *
	 * @param mixed $where
	 * @return string
	 */
	protected function _whereExpr($where)
	{
		if (empty($where)) {
			return $where;
		}
		if (!is_array($where)) {
			$where = array($where);
		}
		foreach ($where as $cond => &$term) {
			// is $cond an int? (i.e. Not a condition)
			if (is_int($cond)) {
				// $term is the full condition
				if ($term instanceof Expr) {
					$term = $term->__toString();
				}
			} else {
				// $cond is the condition with placeholder,
				// and $term is quoted into the condition
				$term = $this->_adapter->quoteInto($cond, $term);
			}
			$term = '(' . $term . ')';
		}

		$where = implode(' AND ', $where);
		return $where;
	}
}