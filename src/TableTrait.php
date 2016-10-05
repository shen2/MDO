<?php
namespace MDO;

trait TableTrait {
    /**
     * 
     * @var Table
     */
    protected static $_defaultTable;
    
    /**
     * @var Table
     */
    protected $_table;
    
    /**
     * 
     * @return Table
     */
    public static function getDefaultTable(){
		if (!isset(self::$_defaultTable)){
			self::$_defaultTable = new Table([
				Table::ADAPTER=> static::$_db,
				Table::SCHEMA => static::$_schema,
				Table::NAME   => static::$_name,
				Table::PRIMARY=> static::$_primary,
				Table::SEQUENCE=>static::$_sequence,
			    Table::ROW_CLASS=>static::class,
				]);
		}
		return self::$_defaultTable;
	}
	
	public static function getTable(){
	    return static::getDefaultTable();
	}
	
	/**
	 * 
	 * @param Table $table
	 */
	public function setTable($table){
	    $this->_table = $table;
	    return $this;
	}
	
	/**
	 * Fetches a new blank row (not from the database).
	 *
	 * @param  array $data OPTIONAL data to populate in the new row.
	 * @param  string $defaultSource OPTIONAL flag to force default values into new row
	 * @return DataObject
	 */
	public static function createRow(array $data = [])
	{
	    return parent::createRow($data)->setTable(static::getDefaultTable());
	}
	
	/* 以下代码是为了向下兼容 */
	/**
	 * Returns an instance of a Select object.
	 *
	 * @param bool $withFromPart Whether or not to include the from part of the select based on the table
	 * @return Select
	 */
	public static function select($withFromPart = Table::SELECT_WITHOUT_FROM_PART)
	{
	    return static::getDefaultTable()->select($withFromPart);
	}
	
	/**
	 * Returns an instance of a Select object.
	 *
	 * @param string|array|Expr $columns
	 * @return Select
	 */
	public static function selectCol($columns = null)
	{
	    return static::getDefaultTable()->selectCol($columns);
	}
	
	/**
	 * Inserts a new row.
	 *
	 * @param  array  $data  Column-value pairs.
	 * @return mixed		 The primary key of the row inserted.
	 */
	public static function insert(array $data)
	{
	    return static::getDefaultTable()->insert($data);
	}
	
	/**
	 * Inserts Delayed a new row.
	 *
	 * @param  array  $data  Column-value pairs.
	 * @return mixed		 The primary key of the row inserted.
	 */
	public static function insertDelayed(array $data)
	{
	    return static::getDefaultTable()->insertDelayed($data);
	}
	
	/**
	 * Inserts Ignore a new row.
	 *
	 * @param  array  $data  Column-value pairs.
	 * @return mixed		 The primary key of the row inserted.
	 */
	public static function insertIgnore(array $data)
	{
	    return static::getDefaultTable()->insertIgnore($data);
	}
	
	/**
	 * 使用insertOnDuplicateKeyUpdate 插入一条记录，插入后不会refresh
	 *
	 * @param  array  $data  Column-value pairs.
	 * @return mixed		 The primary key of the row inserted.
	 */
	public static function insertOrUpdateRow(array $data)
	{
	    return static::getDefaultTable()->insertOrUpdateRow($data);
	}
	
	/**
	 * Inserts Ignore a new row.
	 *
	 * @param  array  $data  Column-value pairs.
	 * @return mixed		 The primary key of the row inserted.
	 */
	public static function insertMulti(array $data)
	{
	    return static::getDefaultTable()->insertMulti($data);
	}
	
	/**
	 * Updates existing rows.
	 *
	 * @param  array		$data  Column-value pairs.
	 * @param  array|string $where An SQL WHERE clause, or an array of SQL WHERE clauses.
	 * @return \mysqli_result
	 */
	public static function update(array $data, $where)
	{
	    return static::getDefaultTable()->update($data, $where);
	}
	
	/**
	 * Deletes existing rows.
	 *
	 * @param  array|string $where SQL WHERE clause(s).
	 * @return \mysqli_result
	 */
	public static function delete($where)
	{
	    return static::getDefaultTable()->delete($where);
	}
	
	/**
	 * Fetches rows by primary key.  The argument specifies one or more primary
	 * key value(s).  To find multiple rows by primary key, the argument must
	 * be an array.
	 *
	 * This method accepts a variable number of arguments.  If the table has a
	 * multi-column primary key, the number of arguments must be the same as
	 * the number of columns in the primary key.  To find multiple rows in a
	 * table with a multi-column primary key, each argument must be an array
	 * with the same number of elements.
	 *
	 * The find() method always returns a Rowset object, even if only one row
	 * was found.
	 *
	 * @param  mixed $key The value(s) of the primary keys.
	 * @return Statement Row(s) matching the criteria.
	 * @throws DataObjectException
	 */
	public static function find()
	{
	    return call_user_func_array([static::getDefaultTable(), 'find'], func_get_args());
	}
	
	/**
	 *
	 * @return \SplFixedArray
	 */
	public static function fetchAllByPrimary($keys){
	    return static::getDefaultTable()->fetchAllByPrimary($keys);
	}
	/*  向下兼容部分代码结束 */
	
	/**
	 * @return mixed The primary key value(s), as an associative array if the
	 *	 key is compound, or a scalar if the key is single-column.
	 */
	protected function _doInsert()
	{
		/**
		 * Execute the INSERT (this may throw an exception)
		 */
		$data = array_intersect_key($this->getArrayCopy(), $this->_modifiedFields);
		$primaryKey = $this->_table->insert($data);

		/**
		 * Normalize the result to an array indexed by primary key column(s).
		 * The table insert() method may return a scalar.
		 */
		if (is_array($primaryKey)) {
			$newPrimaryKey = $primaryKey;
		} else {
			//ZF-6167 Use tempPrimaryKey temporary to avoid that zend encoding fails.
			$tempPrimaryKey = $this->_table->getPrimary();
			$newPrimaryKey = array(current($tempPrimaryKey) => $primaryKey);
		}

		/**
		 * Save the new primary key value in _data.  The primary key may have
		 * been generated by a sequence or auto-increment mechanism, and this
		 * merge should be done before the _postInsert() method is run, so the
		 * new values are available for logging, etc.
		 */
		$this->setFromArray($newPrimaryKey);

		return $primaryKey;
	}

	/**
	 * @return mixed The primary key value(s), as an associative array if the
	 *	 key is compound, or a scalar if the key is single-column.
	 */
	protected function _doUpdate()
	{
		/**
		 * Get expressions for a WHERE clause
		 * based on the primary key value(s).
		 */
		$where = $this->_getWhereQuery(false);

		/**
		 * Compare the data to the modified fields array to discover
		 * which columns have been changed.
		 */
		$diffData = array_intersect_key($this->getArrayCopy(), $this->_modifiedFields);

		/**
		 * Execute the UPDATE (this may throw an exception)
		 * Do this only if data values were changed.
		 * Use the $diffData variable, so the UPDATE statement
		 * includes SET terms only for data values that changed.
		 */
		if (count($diffData) > 0) {
			$this->_table->update($diffData, $where);
		}

		/**
		 * Return the primary key value(s) as an array
		 * if the key is compound or a scalar if the key
		 * is a scalar.
		 */
		$primaryKey = $this->_getPrimaryKey(true);
		if (count($primaryKey) == 1) {
			return current($primaryKey);
		}

		return $primaryKey;
	}

	/**
	 * Deletes existing rows.
	 *
	 * @return int The number of rows deleted.
	 */
	protected function _doDelete()
	{
		$where = $this->_getWhereQuery();
		/**
		 * Execute the DELETE (this may throw an exception)
		 */
		return $this->_table->delete($where);
	}

	/**
	 * Retrieves an associative array of primary keys.
	 *
	 * @param bool $useDirty
	 * @return array
	 */
	protected function _getPrimaryKey($useDirty = true)
	{
	    $primary = array_flip($this->_table->getPrimary());
	    if ($useDirty) {
	        $array = array_intersect_key($this->getArrayCopy(), $primary);
	    } else {
	        $array = array_intersect_key($this->_cleanData, $primary);
	    }
	    if (count($primary) != count($array)) {
	        throw new DataObjectException("The specified Table '".get_class($this->_table)."' does not have the same primary key as the Row");
	    }
	    return $array;
	}
	
	/**
	 * Constructs where statement for retrieving row(s).
	 *
	 * @param bool $useDirty
	 * @return array
	 */
	protected function _getWhereQuery($useDirty = true)
	{
	    $where = array();
	
	    $primaryKey = $this->_getPrimaryKey($useDirty);
	    $db = $this->_table->getAdapter();
	    $tableName = $db->quoteIdentifier($this->_table->getName(), true);
	
	    // retrieve recently updated row using primary keys
	    foreach ($primaryKey as $column => $value) {
	        $columnName = $db->quoteIdentifier($column, true);
	        $where[] = $db->quoteInto("{$tableName}.{$columnName} = ?", $value);
	    }
	    return $where;
	}
	
	/**
	 * Refreshes properties from the database.
	 *
	 * @return void
	 */
	protected function _refresh($real = true)
	{
	    if ($real){
	        return $this->_realRefresh();
	    }
	
	    //并不真的从数据库中查询记录，而只是记录当成是全新的
	    $this->_cleanData = $this->getArrayCopy();
	    $this->_modifiedFields = array();
	}
	
	protected function _realRefresh(){
	    $where = $this->_getWhereQuery();
	    $row = $this->_table->select()
	        ->whereClauses($where)
	        ->fetchRow();
	
	    if (null === $row) {
	        throw new DataObjectException('Cannot refresh row as parent is missing');
	    }
	
	    $this->_cleanData = $row->getArrayCopy();
	    $this->exchangeArray($this->_cleanData);
	
	    $this->_modifiedFields = array();
	}
	
	/**
	 * Allows pre-insert logic to be applied to row.
	 * Subclasses may override this method.
	 *
	 * @return void
	 */
	protected function _insert()
	{
	}
	
	/**
	 * Allows post-insert logic to be applied to row.
	 * Subclasses may override this method.
	 *
	 * @return void
	 */
	protected function _postInsert()
	{
	}
	
	/**
	 * Allows pre-update logic to be applied to row.
	 * Subclasses may override this method.
	 *
	 * @return void
	 */
	protected function _update()
	{
	}
	
	/**
	 * Allows post-update logic to be applied to row.
	 * Subclasses may override this method.
	 *
	 * @return void
	 */
	protected function _postUpdate()
	{
	}
	
	/**
	 * Allows pre-delete logic to be applied to row.
	 * Subclasses may override this method.
	 *
	 * @return void
	 */
	protected function _delete()
	{
	}
	
	/**
	 * Allows post-delete logic to be applied to row.
	 * Subclasses may override this method.
	 *
	 * @return void
	 */
	protected function _postDelete()
	{
	}
}
