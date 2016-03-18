<?php 
namespace MDO;

class Query {

	const DISTINCT	   = 'distinct';
	const COLUMNS		= 'columns';
	const FROM		   = 'from';
	const UNION		  = 'union';
	const INDEX		  = 'index';
	const WHERE		  = 'where';
	const GROUP		  = 'group';
	const HAVING		 = 'having';
	const ORDER		  = 'order';
	const LIMIT_COUNT	= 'limitcount';
	const LIMIT_OFFSET   = 'limitoffset';
	const FOR_UPDATE	 = 'forupdate';
	
	const SQL_WILDCARD   = '*';
	const SQL_SELECT	 = 'SELECT';
	const SQL_UNION	  = 'UNION';
	const SQL_UNION_ALL  = 'UNION ALL';
	const SQL_FROM	   = 'FROM';
	const SQL_WHERE	  = 'WHERE';
        const SQL_FORCE_INDEX = 'FORCE INDEX';
        const SQL_USE_INDEX = 'USE INDEX';
        const SQL_IGNORE_INDEX = 'IGNORE INDEX';
	const SQL_DISTINCT   = 'DISTINCT';
	const SQL_GROUP_BY   = 'GROUP BY';
	const SQL_ORDER_BY   = 'ORDER BY';
	const SQL_HAVING	 = 'HAVING';
	const SQL_FOR_UPDATE = 'FOR UPDATE';
	const SQL_AND		= 'AND';
	const SQL_AS		 = 'AS';
	const SQL_OR		 = 'OR';
	const SQL_ON		 = 'ON';
	const SQL_ASC		= 'ASC';
	const SQL_DESC	   = 'DESC';
	
	/**
	 * Adapter object.
	 *
	 * @var Adapter
	 */
	protected $_adapter;
	
	/**
	 * Table name that created this select object
	 *
	 * @var Table
	 */
	protected $_table;
	
	/**
	 * Bind variables for query
	 *
	 * @var array
	 */
	protected $_bind = array();
	
	/**
	 * The component parts of a SELECT statement.
	 * Initialized to the $_partsInit array in the constructor.
	 *
	 * @var array
	 */
	protected $_parts = array();
	
	public function __construct(Adapter $adapter){
		$this->_adapter = $adapter;
	}

	/**
	 * Implements magic method.
	 *
	 * @return string This object as a SELECT string.
	 */
	public function __toString()
	{
		try {
			$sql = $this->assemble();
		} catch (Exception $e) {
			trigger_error($e->getMessage(), E_USER_WARNING);
			$sql = '';
		}
		return (string)$sql;
	}
	
	/**
	 * Gets the Adapter for this
	 * particular Select object.
	 *
	 * @return Adapter
	 */
	public function getAdapter()
	{
		return $this->_adapter;
	}
	
	/**
	 * Return the table that created this select object
	 *
	 * @return Table
	 */
	public function getTable()
	{
		return $this->_table;
	}
	
	/**
	 * Sets the primary table name and retrieves the table schema.
	 *
	 * @param Table $table
	 * @return Select This Select object.
	 */
	public function setTable($table)
	{
		$this->_table = $table;
	
		return $this;
	}
	
	/**
	 * Get bind variables
	 *
	 * @return array
	 */
	public function getBind()
	{
		return $this->_bind;
	}
	
	/**
	 * Set bind variables
	 *
	 * @param mixed $bind
	 * @return Select
	 */
	public function bind($bind)
	{
		$this->_bind = $bind;
	
		return $this;
	}
	
	/**
	 * Executes the current select object and returns the result
	 *
	 * @param integer $fetchMode OPTIONAL
	 * @return \mysqli_result
	 */
	public function query()
	{
		if (!empty($this->_bind))
			throw new Exception('MDO doesn\'t support bind query now.');
		
		return $this->_adapter->query($this->assemble());
	}
	
	/**
	 * Adds a WHERE condition to the query by AND.
	 *
	 * If a value is passed as the second param, it will be quoted
	 * and replaced into the condition wherever a question-mark
	 * appears. Array values are quoted and comma-separated.
	 *
	 * <code>
	 * // simplest but non-secure
	 * $select->where("id = $id");
	 *
	 * // secure (ID is quoted but matched anyway)
	 * $select->where('id = ?', $id);
	 *
	 * // alternatively, with named binding
	 * $select->where('id = :id');
	 * </code>
	 *
	 * Note that it is more correct to use named bindings in your
	 * queries for values other than strings. When you use named
	 * bindings, don't forget to pass the values when actually
	 * making a query:
	 *
	 * <code>
	 * $db->fetchAll($select, array('id' => 5));
	 * </code>
	 *
	 * @param string   $cond  The WHERE condition.
	 * @param mixed	$value OPTIONAL The value to quote into the condition.
	 * @param int	  $type  OPTIONAL The type of the given value
	 * @return Select This Select object.
	 */
	public function where($cond, $value = null)
	{
		$this->_parts[self::WHERE][] = $this->_where($cond, $value, true);
	
		return $this;
	}
	
	/**
	 * Adds a WHERE condition to the query by OR.
	 *
	 * Otherwise identical to where().
	 *
	 * @param string   $cond  The WHERE condition.
	 * @param mixed	$value OPTIONAL The value to quote into the condition.
	 * @param int	  $type  OPTIONAL The type of the given value
	 * @return Select This Select object.
	 *
	 * @see where()
	 */
	public function orWhere($cond, $value = null)
	{
		$this->_parts[self::WHERE][] = $this->_where($cond, $value, false);
	
		return $this;
	}
	

	/**
	 * Internal function for creating the where clause
	 *
	 * @param string   $condition
	 * @param mixed	$value  optional
	 * @param string   $type   optional
	 * @param boolean  $bool  true = AND, false = OR
	 * @return string  clause
	 */
	protected function _where($condition, $value = null, $bool = true)
	{
		if (count($this->_parts[self::UNION])) {
			throw new SelectException("Invalid use of where clause with " . self::SQL_UNION);
		}

		if ($value !== null) {
			$condition = $this->_adapter->quoteInto($condition, $value);
		}

		$cond = "";
		if ($this->_parts[self::WHERE]) {
			if ($bool === true) {
				$cond = self::SQL_AND . ' ';
			} else {
				$cond = self::SQL_OR . ' ';
			}
		}

		return $cond . "($condition)";
	}
	
	/**
	 * Render WHERE clause
	 *
	 * @return string
	 */
	protected function _renderWhere()
	{
		if ($this->_parts[self::WHERE]) {
			return ' ' . self::SQL_WHERE . ' ' .  implode(' ', $this->_parts[self::WHERE]);
		}
	
		return '';
	}
	
	/**
	 * Convert an array, string, or Expr object
	 * into a string to put in a WHERE clause.
	 *
	 * @param mixed $where
	 * @return string
	 */
	public function whereClauses($where)
	{
		foreach ((array)$where as $cond => &$term) {
			// is $cond an int? (i.e. Not a condition)
			if (is_int($cond)) {
				// $term is the full condition
				$term = $this->_where($term instanceof Expr ? $term->__toString() : $term, null, true);
			} else {
				// $cond is the condition with placeholder,
				// and $term is quoted into the condition
				$term = $this->_where($cond, $term, true);
			}
			$this->_parts[self::WHERE][] = $term;
		}

		return $this;
	}
}
