<?php
namespace MDO;

/**
 * 
 * @author shen2
 * 
 * 从Db_Adapter移动了fetch系列方法到此，
 * 
 * 优化了以下方法
 * fetchAssoc
 * fetchPairs
 * 
 * 增加了以下方法
 * fetchInto
 * fetchClass
 * fetchFunc
 * 
 * 删除了_getDummyTable()和相关代码
 * 删除了$_tableCols
 */
class Select extends Query
{
	const INNER_JOIN	 = 'inner join';
	const LEFT_JOIN	  = 'left join';
	const RIGHT_JOIN	 = 'right join';
	const FULL_JOIN	  = 'full join';
	const CROSS_JOIN	 = 'cross join';
	const NATURAL_JOIN   = 'natural join';
        const STRAIGHT_JOIN   = 'straight_join';

	/**
	 * The initial values for the $_parts array.
	 * NOTE: It is important for the 'FOR_UPDATE' part to be last to ensure
	 * meximum compatibility with database adapters.
	 *
	 * @var array
	 */
	protected $_parts = array(
		self::DISTINCT	 => false,
		self::COLUMNS	  => array(),
		self::UNION		=> array(),
		self::FROM		 => array(),
		self::WHERE		=> array(),
		self::GROUP		=> array(),
		self::HAVING	   => array(),
		self::ORDER		=> array(),
		self::LIMIT_COUNT  => null,
		self::LIMIT_OFFSET => null,
		self::FOR_UPDATE   => false
	);

	/**
	 * Specify legal join types.
	 *
	 * @var array
	 */
	protected static $_joinTypes = array(
		self::INNER_JOIN,
		self::LEFT_JOIN,
		self::RIGHT_JOIN,
		self::FULL_JOIN,
		self::CROSS_JOIN,
		self::NATURAL_JOIN,
            self::STRAIGHT_JOIN,
	);

	/**
	 * Specify legal union types.
	 *
	 * @var array
	 */
	protected static $_unionTypes = array(
		self::SQL_UNION,
		self::SQL_UNION_ALL
	);

	/**
	 * Class constructor
	 *
	 * @param Adapter $adapter
	 */
	public function __construct(Adapter $adapter)
	{
		$this->_adapter = $adapter;
	}

	/**
	 * Makes the query SELECT DISTINCT.
	 *
	 * @param bool $flag Whether or not the SELECT is DISTINCT (default true).
	 * @return Select This Select object.
	 */
	public function distinct($flag = true)
	{
		$this->_parts[self::DISTINCT] = (bool) $flag;
		return $this;
	}

	/**
	 * Adds a FROM table and optional columns to the query.
	 *
	 * The first parameter $name can be a simple string, in which case the
	 * correlation name is generated automatically.  If you want to specify
	 * the correlation name, the first parameter must be an associative
	 * array in which the key is the correlation name, and the value is
	 * the physical table name.  For example, array('alias' => 'table').
	 * The correlation name is prepended to all columns fetched for this
	 * table.
	 *
	 * The second parameter can be a single string or Expr object,
	 * or else an array of strings or Expr objects.
	 *
	 * The first parameter can be null or an empty string, in which case
	 * no correlation name is generated or prepended to the columns named
	 * in the second parameter.
	 *
	 * @param  array|string|Expr $name The table name or an associative array
	 *										 relating correlation name to table name.
	 * @param  array|string|Expr $cols The columns to select from this table.
	 * @param  string $schema The schema name to specify, if any.
	 * @return Select This Select object.
	 */
	public function from($name, $cols = '*', $schema = null)
	{
		return $this->_join(self::FROM, $name, null, $cols, $schema);
	}

	/**
	 * Specifies the columns used in the FROM clause.
	 *
	 * The parameter can be a single string or Expr object,
	 * or else an array of strings or Expr objects.
	 *
	 * @param  array|string|Expr $cols The columns to select from this table.
	 * @param  string $correlationName Correlation name of target table. OPTIONAL
	 * @return Select This Select object.
	 */
	public function columns($cols = '*', $correlationName = null)
	{
		if ($correlationName === null && count($this->_parts[self::FROM])) {
			$correlationNameKeys = array_keys($this->_parts[self::FROM]);
			$correlationName = current($correlationNameKeys);
		}

		if (!array_key_exists($correlationName, $this->_parts[self::FROM])) {
			throw new SelectException("No table has been specified for the FROM clause");
		}

		$this->_tableCols($correlationName, $cols);

		return $this;
	}

	/**
	 * Adds a UNION clause to the query.
	 *
	 * The first parameter has to be an array of Select or
	 * sql query strings.
	 *
	 * <code>
	 * $sql1 = $db->select();
	 * $sql2 = "SELECT ...";
	 * $select = $db->select()
	 *	  ->union(array($sql1, $sql2))
	 *	  ->order("id");
	 * </code>
	 *
	 * @param  array $select Array of select clauses for the union.
	 * @return Select This Select object.
	 */
	public function union($select = array(), $type = self::SQL_UNION)
	{
		if (!is_array($select)) {
			throw new SelectException(
				"union() only accepts an array of Select instances of sql query strings."
			);
		}

		if (!in_array($type, self::$_unionTypes)) {
			throw new SelectException("Invalid union type '{$type}'");
		}

		foreach ($select as $target) {
			$this->_parts[self::UNION][] = array($target, $type);
		}

		return $this;
	}

	/**
	 * Adds a JOIN table and columns to the query.
	 *
	 * The $name and $cols parameters follow the same logic
	 * as described in the from() method.
	 *
	 * @param  array|string|Expr $name The table name.
	 * @param  string $cond Join on this condition.
	 * @param  array|string $cols The columns to select from the joined table.
	 * @param  string $schema The database name to specify, if any.
	 * @return Select This Select object.
	 */
	public function join($name, $cond, $cols = self::SQL_WILDCARD, $schema = null)
	{
		return $this->joinInner($name, $cond, $cols, $schema);
	}

	/**
	 * Add an INNER JOIN table and colums to the query
	 * Rows in both tables are matched according to the expression
	 * in the $cond argument.  The result set is comprised
	 * of all cases where rows from the left table match
	 * rows from the right table.
	 *
	 * The $name and $cols parameters follow the same logic
	 * as described in the from() method.
	 *
	 * @param  array|string|Expr $name The table name.
	 * @param  string $cond Join on this condition.
	 * @param  array|string $cols The columns to select from the joined table.
	 * @param  string $schema The database name to specify, if any.
	 * @return Select This Select object.
	 */
	public function joinInner($name, $cond, $cols = self::SQL_WILDCARD, $schema = null)
	{
		return $this->_join(self::INNER_JOIN, $name, $cond, $cols, $schema);
	}

	/**
	 * Add a LEFT OUTER JOIN table and colums to the query
	 * All rows from the left operand table are included,
	 * matching rows from the right operand table included,
	 * and the columns from the right operand table are filled
	 * with NULLs if no row exists matching the left table.
	 *
	 * The $name and $cols parameters follow the same logic
	 * as described in the from() method.
	 *
	 * @param  array|string|Expr $name The table name.
	 * @param  string $cond Join on this condition.
	 * @param  array|string $cols The columns to select from the joined table.
	 * @param  string $schema The database name to specify, if any.
	 * @return Select This Select object.
	 */
	public function joinLeft($name, $cond, $cols = self::SQL_WILDCARD, $schema = null)
	{
		return $this->_join(self::LEFT_JOIN, $name, $cond, $cols, $schema);
	}

	/**
	 * Add a RIGHT OUTER JOIN table and colums to the query.
	 * Right outer join is the complement of left outer join.
	 * All rows from the right operand table are included,
	 * matching rows from the left operand table included,
	 * and the columns from the left operand table are filled
	 * with NULLs if no row exists matching the right table.
	 *
	 * The $name and $cols parameters follow the same logic
	 * as described in the from() method.
	 *
	 * @param  array|string|Expr $name The table name.
	 * @param  string $cond Join on this condition.
	 * @param  array|string $cols The columns to select from the joined table.
	 * @param  string $schema The database name to specify, if any.
	 * @return Select This Select object.
	 */
	public function joinRight($name, $cond, $cols = self::SQL_WILDCARD, $schema = null)
	{
		return $this->_join(self::RIGHT_JOIN, $name, $cond, $cols, $schema);
	}

	/**
	 * Add a FULL OUTER JOIN table and colums to the query.
	 * A full outer join is like combining a left outer join
	 * and a right outer join.  All rows from both tables are
	 * included, paired with each other on the same row of the
	 * result set if they satisfy the join condition, and otherwise
	 * paired with NULLs in place of columns from the other table.
	 *
	 * The $name and $cols parameters follow the same logic
	 * as described in the from() method.
	 *
	 * @param  array|string|Expr $name The table name.
	 * @param  string $cond Join on this condition.
	 * @param  array|string $cols The columns to select from the joined table.
	 * @param  string $schema The database name to specify, if any.
	 * @return Select This Select object.
	 */
	public function joinFull($name, $cond, $cols = self::SQL_WILDCARD, $schema = null)
	{
		return $this->_join(self::FULL_JOIN, $name, $cond, $cols, $schema);
	}

        /**
         * Add a STRAIGHT JOIN table and colums to the query.
         * The $name and $cols parameters follow the same logic
         * as described in the from() method.
         *
         * @param  array|string|Expr $name The table name.
         * @param  string $cond Join on this condition.
         * @param  array|string $cols The columns to select from the joined table.
         * @param  string $schema The database name to specify, if any.
         * @return Select This Select object.
         */
        public function joinStraight($name, $cond, $cols = self::SQL_WILDCARD, $schema = null)
        {
            return $this->_join(self::STRAIGHT_JOIN, $name, $cond, $cols, $schema);
        }

	/**
	 * Add a CROSS JOIN table and colums to the query.
	 * A cross join is a cartesian product; there is no join condition.
	 *
	 * The $name and $cols parameters follow the same logic
	 * as described in the from() method.
	 *
	 * @param  array|string|Expr $name The table name.
	 * @param  array|string $cols The columns to select from the joined table.
	 * @param  string $schema The database name to specify, if any.
	 * @return Select This Select object.
	 */
	public function joinCross($name, $cols = self::SQL_WILDCARD, $schema = null)
	{
		return $this->_join(self::CROSS_JOIN, $name, null, $cols, $schema);
	}

	/**
	 * Add a NATURAL JOIN table and colums to the query.
	 * A natural join assumes an equi-join across any column(s)
	 * that appear with the same name in both tables.
	 * Only natural inner joins are supported by this API,
	 * even though SQL permits natural outer joins as well.
	 *
	 * The $name and $cols parameters follow the same logic
	 * as described in the from() method.
	 *
	 * @param  array|string|Expr $name The table name.
	 * @param  array|string $cols The columns to select from the joined table.
	 * @param  string $schema The database name to specify, if any.
	 * @return Select This Select object.
	 */
	public function joinNatural($name, $cols = self::SQL_WILDCARD, $schema = null)
	{
		return $this->_join(self::NATURAL_JOIN, $name, null, $cols, $schema);
	}

        public function forceIndex($indexList, $for = null, $table = null){
            $indexSql = self::SQL_FORCE_INDEX . ($for ? ' FOR ' . $for : '') . ' (' . implode(',', (array) $indexList) . ')';
            return $this->_index($indexSql, $table);
        }

        public function useIndex($indexList, $for = null, $table = null){
            $indexSql = self::SQL_USE_INDEX . ($for ? ' FOR ' . $for : '') . ' (' . implode(',', (array) $indexList) . ')';
            return $this->_index($indexSql, $table);
        }

        public function ignoreIndex($indexList, $for = null, $table = null){
            $indexSql = self::SQL_IGNORE_INDEX . ($for ? ' FOR ' . $for : '') . ' (' . implode(',', (array) $indexList) . ')';
            return $this->_index($indexSql, $table);
        }

        public function _index($indexSql, $table = null){
            $table = $table ? $table : key($this->_parts[self::FROM]);
            $this->_parts[self::FROM][$table]['index'][] = $indexSql;
            return $this;
        }

	/**
	 * Adds grouping to the query.
	 *
	 * @param  array|string $spec The column(s) to group by.
	 * @return Select This Select object.
	 */
	public function group($spec)
	{
		if (!is_array($spec)) {
			$spec = array($spec);
		}

		foreach ($spec as $val) {
			if (preg_match('/\(.*\)/', (string) $val)) {
				$val = new Expr($val);
			}
			$this->_parts[self::GROUP][] = $val;
		}

		return $this;
	}

	/**
	 * Adds a HAVING condition to the query by AND.
	 *
	 * If a value is passed as the second param, it will be quoted
	 * and replaced into the condition wherever a question-mark
	 * appears. See {@link where()} for an example
	 *
	 * @param string $cond The HAVING condition.
	 * @param mixed	$value OPTIONAL The value to quote into the condition.
	 * @param int	  $type  OPTIONAL The type of the given value
	 * @return Select This Select object.
	 */
	public function having($cond, $value = null)
	{
		if ($value !== null) {
			$cond = $this->_adapter->quoteInto($cond, $value);
		}

		if ($this->_parts[self::HAVING]) {
			$this->_parts[self::HAVING][] = self::SQL_AND . " ($cond)";
		} else {
			$this->_parts[self::HAVING][] = "($cond)";
		}

		return $this;
	}

	/**
	 * Adds a HAVING condition to the query by OR.
	 *
	 * Otherwise identical to orHaving().
	 *
	 * @param string $cond The HAVING condition.
	 * @param mixed	$value OPTIONAL The value to quote into the condition.
	 * @param int	  $type  OPTIONAL The type of the given value
	 * @return Select This Select object.
	 *
	 * @see having()
	 */
	public function orHaving($cond, $value = null)
	{
		if ($value !== null) {
			$cond = $this->_adapter->quoteInto($cond, $value);
		}

		if ($this->_parts[self::HAVING]) {
			$this->_parts[self::HAVING][] = self::SQL_OR . " ($cond)";
		} else {
			$this->_parts[self::HAVING][] = "($cond)";
		}

		return $this;
	}

	/**
	 * Adds a row order to the query.
	 *
	 * @param mixed $spec The column(s) and direction to order by.
	 * @return Select This Select object.
	 */
	public function order($spec)
	{
		if (!is_array($spec)) {
			$spec = array($spec);
		}

		// force 'ASC' or 'DESC' on each order spec, default is ASC.
		foreach ($spec as $val) {
			if ($val instanceof Expr) {
				$expr = $val->__toString();
				if (empty($expr)) {
					continue;
				}
				$this->_parts[self::ORDER][] = $val;
			} else {
				if (empty($val)) {
					continue;
				}
				$direction = self::SQL_ASC;
				if (preg_match('/(.*\W)(' . self::SQL_ASC . '|' . self::SQL_DESC . ')\b/si', $val, $matches)) {
					$val = trim($matches[1]);
					$direction = $matches[2];
				}
				if (preg_match('/\(.*\)/', $val)) {
					$val = new Expr($val);
				}
				$this->_parts[self::ORDER][] = array($val, $direction);
			}
		}

		return $this;
	}

	/**
	 * Sets a limit count and offset to the query.
	 *
	 * @param int $count OPTIONAL The number of rows to return.
	 * @param int $offset OPTIONAL Start returning after this many rows.
	 * @return Select This Select object.
	 */
	public function limit($count = null, $offset = null)
	{
		$this->_parts[self::LIMIT_COUNT]  = (int) $count;
		$this->_parts[self::LIMIT_OFFSET] = (int) $offset;
		return $this;
	}

	/**
	 * Sets the limit and count by page number.
	 *
	 * @param int $page Limit results to this page number.
	 * @param int $rowCount Use this many rows per page.
	 * @return Select This Select object.
	 */
	public function limitPage($page, $rowCount)
	{
		$page	 = ($page > 0)	 ? $page	 : 1;
		$rowCount = ($rowCount > 0) ? $rowCount : 1;
		$this->_parts[self::LIMIT_COUNT]  = (int) $rowCount;
		$this->_parts[self::LIMIT_OFFSET] = (int) $rowCount * ($page - 1);
		return $this;
	}

	/**
	 * Makes the query SELECT FOR UPDATE.
	 *
	 * @param bool $flag Whether or not the SELECT is FOR UPDATE (default true).
	 * @return Select This Select object.
	 */
	public function forUpdate($flag = true)
	{
		$this->_parts[self::FOR_UPDATE] = (bool) $flag;
		return $this;
	}

	/**
	 * Get part of the structured information for the currect query.
	 *
	 * @param string $part
	 * @return mixed
	 * @throws SelectException
	 */
	public function getPart($part)
	{
		$part = strtolower($part);
		if (!array_key_exists($part, $this->_parts)) {
			throw new SelectException("Invalid Select part '$part'");
		}
		return $this->_parts[$part];
	}

	/**
	 * Converts this object to an SQL SELECT string.
	 *
	 * @return string|null This object as a SELECT string. (or null if a string cannot be produced.)
	 */
	public function assemble()
	{
		/**
		 * Performs a validation on the select query before passing back to the parent class.
		 * Ensures that only columns from the primary DataObject are returned in the result.
		 */
		if (isset($this->_table) && count($this->_parts[self::UNION]) == 0) {
			$info = $this->_table->info();
			
			// If no fields are specified we assume all fields from primary table
			if (empty($this->_parts[Select::COLUMNS])) {
				$this->joinInner($info[Table::NAME], null, self::SQL_WILDCARD, $info[Table::SCHEMA]);
			}
		}
		
		$sql = self::SQL_SELECT;
		foreach (array_keys($this->_parts) as $part) {
			$method = '_render' . ucfirst($part);
			if (method_exists($this, $method)) {
				$sql .= $this->$method();
			}
		}
		return $sql;
	}

	/**
	 * Clear parts of the Select object, or an individual part.
	 *
	 * @param string $part OPTIONAL
	 * @return Select
	 */
	public function reset($part = null)
	{
		static $partsInit = null;
		
		if ($partsInit === null){
			$partsInit = (new self($this->_adapter))->_parts;
		}
		
		if ($part == null) {
			$this->_parts = $partsInit;
		} else if (array_key_exists($part, $partsInit)) {
			$this->_parts[$part] = $partsInit[$part];
		}
		return $this;
	}

	/**
	 * Populate the {@link $_parts} 'join' key
	 *
	 * Does the dirty work of populating the join key.
	 *
	 * The $name and $cols parameters follow the same logic
	 * as described in the from() method.
	 *
	 * @param  null|string $type Type of join; inner, left, and null are currently supported
	 * @param  array|string|Expr $name Table name
	 * @param  string $cond Join on this condition
	 * @param  array|string $cols The columns to select from the joined table
	 * @param  string $schema The database name to specify, if any.
	 * @return Select This Select object
	 * @throws SelectException
	 */
	protected function _join($type, $name, $cond, $cols, $schema = null)
	{
		if (!in_array($type, self::$_joinTypes) && $type != self::FROM) {
			throw new SelectException("Invalid join type '$type'");
		}

		if (count($this->_parts[self::UNION])) {
			throw new SelectException("Invalid use of table with " . self::SQL_UNION);
		}

		if (empty($name)) {
			$correlationName = $tableName = '';
		} else if (is_array($name)) {
			// Must be array($correlationName => $tableName) or array($ident, ...)
			foreach ($name as $_correlationName => $_tableName) {
				if (is_string($_correlationName)) {
					// We assume the key is the correlation name and value is the table name
					$tableName = $_tableName;
					$correlationName = $_correlationName;
				} else {
					// We assume just an array of identifiers, with no correlation name
					$tableName = $_tableName;
					$correlationName = $this->_uniqueCorrelation($tableName);
				}
				break;
			}
		} else if ($name instanceof Expr|| $name instanceof Select) {
			$tableName = $name;
			$correlationName = $this->_uniqueCorrelation('t');
		} else if (preg_match('/^(.+)\s+AS\s+(.+)$/i', $name, $m)) {
			$tableName = $m[1];
			$correlationName = $m[2];
		} else {
			$tableName = $name;
			$correlationName = $this->_uniqueCorrelation($tableName);
		}

		// Schema from table name overrides schema argument
		if (!is_object($tableName) && false !== strpos($tableName, '.')) {
			list($schema, $tableName) = explode('.', $tableName);
		}

		$lastFromCorrelationName = null;
		if (!empty($correlationName)) {
			if (array_key_exists($correlationName, $this->_parts[self::FROM])) {
				throw new SelectException("You cannot define a correlation name '$correlationName' more than once");
			}

			if ($type == self::FROM) {
				// append this from after the last from joinType
				$tmpFromParts = $this->_parts[self::FROM];
				$this->_parts[self::FROM] = array();
				// move all the froms onto the stack
				while ($tmpFromParts) {
					$currentCorrelationName = key($tmpFromParts);
					if ($tmpFromParts[$currentCorrelationName]['joinType'] != self::FROM) {
						break;
					}
					$lastFromCorrelationName = $currentCorrelationName;
					$this->_parts[self::FROM][$currentCorrelationName] = array_shift($tmpFromParts);
				}
			} else {
				$tmpFromParts = array();
			}
			$this->_parts[self::FROM][$correlationName] = array(
				'joinType'	  => $type,
				'schema'		=> $schema,
				'tableName'	 => $tableName,
				'joinCondition' => $cond
				);
			while ($tmpFromParts) {
				$currentCorrelationName = key($tmpFromParts);
				$this->_parts[self::FROM][$currentCorrelationName] = array_shift($tmpFromParts);
			}
		}

		// add to the columns from this joined table
		if ($type == self::FROM && $lastFromCorrelationName == null) {
			$lastFromCorrelationName = true;
		}
		$this->_tableCols($correlationName, $cols, $lastFromCorrelationName);

		return $this;
	}

	/**
	 * Handle JOIN... USING... syntax
	 *
	 * This is functionality identical to the existing JOIN methods, however
	 * the join condition can be passed as a single column name. This method
	 * then completes the ON condition by using the same field for the FROM
	 * table and the JOIN table.
	 *
	 * <code>
	 * $select = $db->select()->from('table1')
	 *						->joinUsing('table2', 'column1');
	 *
	 * // SELECT * FROM table1 JOIN table2 ON table1.column1 = table2.column2
	 * </code>
	 *
	 * These joins are called by the developer simply by adding 'Using' to the
	 * method name. E.g.
	 * * joinUsing
	 * * joinInnerUsing
	 * * joinFullUsing
	 * * joinRightUsing
	 * * joinLeftUsing
	 *
	 * @return Select This Select object.
	 */
	public function _joinUsing($type, $name, $cond, $cols = '*', $schema = null)
	{
		if (empty($this->_parts[self::FROM])) {
			throw new SelectException("You can only perform a joinUsing after specifying a FROM table");
		}

		$join  = $this->_adapter->quoteIdentifier(key($this->_parts[self::FROM]), true);
		$from  = $this->_adapter->quoteIdentifier($this->_uniqueCorrelation($name), true);

		$cond1 = $from . '.' . $cond;
		$cond2 = $join . '.' . $cond;
		$cond  = $cond1 . ' = ' . $cond2;

		return $this->_join($type, $name, $cond, $cols, $schema);
	}

	/**
	 * Generate a unique correlation name
	 *
	 * @param string|array $name A qualified identifier.
	 * @return string A unique correlation name.
	 */
	private function _uniqueCorrelation($name)
	{
		if (is_array($name)) {
			$c = end($name);
		} else {
			// Extract just the last name of a qualified table name
			$dot = strrpos($name,'.');
			$c = ($dot === false) ? $name : substr($name, $dot+1);
		}
		for ($i = 2; array_key_exists($c, $this->_parts[self::FROM]); ++$i) {
			$c = $name . '_' . (string) $i;
		}
		return $c;
	}

	/**
	 * Adds to the internal table-to-column mapping array.
	 *
	 * @param  string $tbl The table/join the columns come from.
	 * @param  array|string $cols The list of columns; preferably as
	 * an array, but possibly as a string containing one column.
	 * @param  bool|string True if it should be prepended, a correlation name if it should be inserted
	 * @return void
	 */
	protected function _tableCols($correlationName, $cols, $afterCorrelationName = null)
	{
		if (!is_array($cols)) {
			$cols = array($cols);
		}

		if ($correlationName == null) {
			$correlationName = '';
		}

		$columnValues = array();

		foreach (array_filter($cols) as $alias => $col) {
			$currentCorrelationName = $correlationName;
			if (is_string($col)) {
				// Check for a column matching "<column> AS <alias>" and extract the alias name
				if (preg_match('/^(.+)\s+' . self::SQL_AS . '\s+(.+)$/i', $col, $m)) {
					$col = $m[1];
					$alias = $m[2];
				}
				// Check for columns that look like functions and convert to Expr
				if (preg_match('/\(.*\)/', $col)) {
					$col = new Expr($col);
				} elseif (preg_match('/(.+)\.(.+)/', $col, $m)) {
					$currentCorrelationName = $m[1];
					$col = $m[2];
				}
			}
			$columnValues[] = array($currentCorrelationName, $col, is_string($alias) ? $alias : null);
		}

		if ($columnValues) {

			// should we attempt to prepend or insert these values?
			if ($afterCorrelationName === true || is_string($afterCorrelationName)) {
				$tmpColumns = $this->_parts[self::COLUMNS];
				$this->_parts[self::COLUMNS] = array();
			} else {
				$tmpColumns = array();
			}

			// find the correlation name to insert after
			if (is_string($afterCorrelationName)) {
				while ($tmpColumns) {
					$this->_parts[self::COLUMNS][] = $currentColumn = array_shift($tmpColumns);
					if ($currentColumn[0] == $afterCorrelationName) {
						break;
					}
				}
			}

			// apply current values to current stack
			foreach ($columnValues as $columnValue) {
				array_push($this->_parts[self::COLUMNS], $columnValue);
			}

			// finish ensuring that all previous values are applied (if they exist)
			while ($tmpColumns) {
				array_push($this->_parts[self::COLUMNS], array_shift($tmpColumns));
			}
		}
	}

	/**
	 * Return a quoted schema name
	 *
	 * @param string   $schema  The schema name OPTIONAL
	 * @return string|null
	 */
	protected function _getQuotedSchema($schema = null)
	{
		if ($schema === null) {
			return null;
		}
		return $this->_adapter->quoteIdentifier($schema, true) . '.';
	}

	/**
	 * Return a quoted table name
	 *
	 * @param string   $tableName		The table name
	 * @param string   $correlationName  The correlation name OPTIONAL
	 * @return string
	 */
	protected function _getQuotedTable($tableName, $correlationName = null)
	{
		return $this->_adapter->quoteTableAs($tableName, $correlationName, true);
	}

	/**
	 * Render DISTINCT clause
	 *
	 * @param string   $sql SQL query
	 * @return string
	 */
	protected function _renderDistinct()
	{
		if ($this->_parts[self::DISTINCT]) {
			return ' ' . self::SQL_DISTINCT;
		}

		return '';
	}

	/**
	 * Render DISTINCT clause
	 *
	 * @param string   $sql SQL query
	 * @return string|null
	 */
	protected function _renderColumns()
	{
		if (!count($this->_parts[self::COLUMNS])) {
			return null;
		}

		$columns = array();
		foreach ($this->_parts[self::COLUMNS] as $columnEntry) {
			list($correlationName, $column, $alias) = $columnEntry;
			if ($column instanceof Expr) {
				$columns[] = $this->_adapter->quoteColumnAs($column, $alias, true);
			} else {
				if ($column == self::SQL_WILDCARD) {
					$column = new Expr(self::SQL_WILDCARD);
					$alias = null;
				}
				if (empty($correlationName)) {
					$columns[] = $this->_adapter->quoteColumnAs($column, $alias, true);
				} else {
					$columns[] = $this->_adapter->quoteColumnAs(array($correlationName, $column), $alias, true);
				}
			}
		}

		return ' ' . implode(', ', $columns);
	}

	/**
	 * Render FROM clause
	 *
	 * @return string
	 */
	protected function _renderFrom()
	{
		$from = array();

		foreach ($this->_parts[self::FROM] as $correlationName => $table) {
			$tmp = '';

			$joinType = ($table['joinType'] == self::FROM) ? self::INNER_JOIN : $table['joinType'];

			// Add join clause (if applicable)
			if (! empty($from)) {
				$tmp .= ' ' . strtoupper($joinType) . ' ';
			}

			$tmp .= $this->_getQuotedSchema($table['schema']);
			$tmp .= $this->_getQuotedTable($table['tableName'], $correlationName);

                        //added by zhu
                        if (!empty($this->_parts[self::FROM][$correlationName]['index'])){
                            $tmp .= ' ' . implode(' ', $this->_parts[self::FROM][$correlationName]['index']);
                        }

			// Add join conditions (if applicable)
			if (!empty($from) && ! empty($table['joinCondition'])) {
				$tmp .= ' ' . self::SQL_ON . ' ' . $table['joinCondition'];
			}

			// Add the table name and condition add to the list
			$from[] = $tmp;
		}

		// Add the list of all joins
		if (!empty($from)) {
			return ' ' . self::SQL_FROM . ' ' . implode("\n", $from);
		}

		return '';
	}

	/**
	 * Render UNION query
	 *
	 * @return string
	 */
	protected function _renderUnion()
	{
		$sql = '';
		if ($this->_parts[self::UNION]) {
			$parts = count($this->_parts[self::UNION]);
			foreach ($this->_parts[self::UNION] as $cnt => $union) {
				list($target, $type) = $union;
				if ($target instanceof Select) {
					$target = $target->assemble();
				}
				$sql .= $target;
				if ($cnt < $parts - 1) {
					$sql .= ' ' . $type . ' ';
				}
			}
		}

		return $sql;
	}


	/**
	 * Render GROUP clause
	 *
	 * @return string
	 */
	protected function _renderGroup()
	{
		if ($this->_parts[self::FROM] && $this->_parts[self::GROUP]) {
			$group = array();
			foreach ($this->_parts[self::GROUP] as $term) {
				$group[] = $this->_adapter->quoteIdentifier($term, true);
			}
			return ' ' . self::SQL_GROUP_BY . ' ' . implode(",\n\t", $group);
		}

		return '';
	}

	/**
	 * Render HAVING clause
	 *
	 * @return string
	 */
	protected function _renderHaving()
	{
		if ($this->_parts[self::FROM] && $this->_parts[self::HAVING]) {
			return ' ' . self::SQL_HAVING . ' ' . implode(' ', $this->_parts[self::HAVING]);
		}

		return '';
	}

	/**
	 * Render ORDER clause
	 *
	 * @return string
	 */
	protected function _renderOrder()
	{
		if ($this->_parts[self::ORDER]) {
			$order = array();
			foreach ($this->_parts[self::ORDER] as $term) {
				if (is_array($term)) {
					if(is_numeric($term[0]) && strval(intval($term[0])) == $term[0]) {
						$order[] = (int)trim($term[0]) . ' ' . $term[1];
					} else {
						$order[] = $this->_adapter->quoteIdentifier($term[0], true) . ' ' . $term[1];
					}
				} else if (is_numeric($term) && strval(intval($term)) == $term) {
					$order[] = (int)trim($term);
				} else {
					$order[] = $this->_adapter->quoteIdentifier($term, true);
				}
			}
			return  ' ' . self::SQL_ORDER_BY . ' ' . implode(', ', $order);
		}

		return '';
	}

	/**
	 * Render LIMIT OFFSET clause
	 *
	 * @return string
	 */
	protected function _renderLimitoffset()
	{
		$sql = '';
		$count = 0;
		$offset = 0;

		if (!empty($this->_parts[self::LIMIT_OFFSET])) {
			$offset = (int) $this->_parts[self::LIMIT_OFFSET];
			$count = PHP_INT_MAX;
		}

		if (!empty($this->_parts[self::LIMIT_COUNT])) {
			$count = (int) $this->_parts[self::LIMIT_COUNT];
		}

		/*
		 * Add limits clause
		 * 从adapter中移动过来
		 */
		if ($count > 0) {
			$sql .= " LIMIT $count";
			if ($offset > 0) {
				$sql .= " OFFSET $offset";
			}
		}

		return $sql;
	}

	/**
	 * Render FOR UPDATE clause
	 *
	 * @return string
	 */
	protected function _renderForupdate()
	{
		if ($this->_parts[self::FOR_UPDATE]) {
			return ' ' . self::SQL_FOR_UPDATE;
		}

		return '';
	}

	/**
	 * Turn magic function calls into non-magic function calls
	 * for joinUsing syntax
	 *
	 * @param string $method
	 * @param array $args OPTIONAL Select query modifier
	 * @return Select
	 * @throws SelectException If an invalid method is called.
	 */
	public function __call($method, array $args)
	{
		$matches = array();

		/**
		 * Recognize methods for Has-Many cases:
		 * findParent<Class>()
		 * findParent<Class>By<Rule>()
		 * Use the non-greedy pattern repeat modifier e.g. \w+?
		 */
		if (preg_match('/^join([a-zA-Z]*?)Using$/', $method, $matches)) {
			$type = strtolower($matches[1]);
			if ($type) {
				$type .= ' join';
				if (!in_array($type, self::$_joinTypes)) {
					throw new SelectException("Unrecognized method '$method()'");
				}
				if (in_array($type, array(self::CROSS_JOIN, self::NATURAL_JOIN))) {
					throw new SelectException("Cannot perform a joinUsing with method '$method()'");
				}
			} else {
				$type = self::INNER_JOIN;
			}
			array_unshift($args, $type);
			return call_user_func_array(array($this, '_joinUsing'), $args);
		}

		throw new SelectException("Unrecognized method '$method()'");
	}
	
	/**
	 * @return Statement
	 */
	public function dispatch($storeResult = true){
		return $this->_adapter->newStatement($this, $storeResult);
	}
	
	/**
	 * Fetches all SQL result rows as a generator.
	 * Uses the current fetchMode for the adapter.
	 *
	 * @param mixed				 $fetchMode Override current fetch mode.
	 * @return \Generator
	 */
	public function yieldAll()
	{
	    if (!isset($this->_table)){
	        throw new SelectException("_table is required");
	    }
	    
	    return $this->_adapter->newStatement($this)->getDataObjectGenerator($this->_table, $this->isReadOnly());
	}

	/**
	 * Fetches all SQL result rows as a generator.
	 *
	 * The first column is the key, the entire row array is the
	 * value.  You should construct the query to be sure that
	 * the first column contains unique values, or else
	 * rows with duplicate values in the first column will
	 * overwrite previous data.
	 *
	 * @return \Generator
	 */
	public function yieldAssoc()
	{
		return $this->_adapter->newStatement($this)->getAssocGenerator();
	}

	/**
	 * Fetches the first column of all SQL result rows as a generator.
	 *
	 * @return \Generator
	 */
	public function yieldCol()
	{
		return $this->_adapter->newStatement($this)->getColumnGenerator(0);
	}
	
	/**
	 * Fetches $name field of all SQL result rows as a generator.
	 *
	 * @return \Generator
	 */
	public function yieldField($name)
	{
		return $this->_adapter->newStatement($this)->getFieldGenerator($name);
	}

	/**
	 * Fetches all SQL result rows as a generator of key-value pairs.
	 *
	 * The first column is the key, the second column is the
	 * value.
	 *
	 * @return \Generator
	 */
	public function yieldPairs()
	{
		return $this->_adapter->newStatement($this)->getKeyPairGenerator();
	}
	
	/**
	 * 以回调函数的方式获取
	 * @param string $func
	 * @return \Generator
	 */
	public function yieldFunc($func){
		return $this->_adapter->newStatement($this)->getFuncGenerator($func);
	}
	
	/**
	 * 以自定义类的方式获取
	 * @param string $class
	 * @param array $ctor_args
	 * @return \Generator
	 */
	public function yieldClass($class, $ctor_args = array()){
		return $this->_adapter->newStatement($this)->getObjectGenerator($class, $ctor_args);
	}
	
	/**
	 * 
	 * @return array
	 */
	public function fetchAll()
	{
	    if (!isset($this->_table)){
	        throw new SelectException("_table is required");
	    }
	    
	    return $this->_adapter->newStatement($this, false)->getDataObjectArray($this->_table, $this->isReadOnly());
	}
	
	/**
	 * Fetches all SQL result rows as an array.
	 *
	 * The first column is the key, the entire row array is the
	 * value.  You should construct the query to be sure that
	 * the first column contains unique values, or else
	 	* rows with duplicate values in the first column will
	 * overwrite previous data.
	 *
	 * @return array
	 */
	public function fetchAssoc()
	{
		return $this->_adapter->newStatement($this, false)->getAssocArray();
	}
	
	/**
	 * Fetches the first column of all SQL result rows as an array.
	 *
	 * @return array
	 */
	public function fetchCol()
	{
		return $this->_adapter->newStatement($this, false)->getColumnArray(0);
	}
	
	/**
	 * Fetches $name field of all SQL result rows as an array.
	 *
	 * @param string $name
	 * @return array
	 */
	public function fetchField($name)
	{
		return $this->_adapter->newStatement($this, false)->getFieldArray($name);
	}
	
	/**
	 * Fetches all SQL result rows as an array of key-value pairs.
	 *
	 * The first column is the key, the second column is the
	 * value.
	 *
	 * @return array
	 */
	public function fetchPairs()
	{
		return $this->_adapter->newStatement($this, false)->getKeyPairArray();
	}
	
	/**
	 * 将第一列作为key，所有的列作为value
	 * @return array
	 */
	public function fetchAssocMap()
	{
		return $this->_adapter->newStatement($this, false)->getAssocMapArray();
	}
	
	/**
	 * 以回调函数的方式获取
	 * @param string $func
	 * @return array
	 */
	public function fetchFunc($func){
		return $this->_adapter->newStatement($this, false)->getFuncArray($func);
	}
	
	/**
	 * 以自定义类的方式获取
	 * @param string $class
	 * @param array $ctor_args
	 * @return array
	 */
	public function fetchClass($class, $ctor_args = array()){
		return $this->_adapter->newStatement($this, false)->getObjectArray($class, $ctor_args);
	}
	
	/**
	 * Fetches the first row of the SQL result.
	 * Uses the current fetchMode for the adapter.
	 *
	 * @return array|DataObject
	 */
	public function fetchRow()
	{
		$this->_parts[self::LIMIT_COUNT]  = 1;
		$result = $this->_adapter->newStatement($this, false)->getResult();
		$data = $result->fetch_assoc();
		$result->close();
		if (isset($this->_table) && $data !== null){
			$rowClass = $this->_table->getRowClass();
			return (new $rowClass($data, true, $this->isReadOnly()))->setTable($this->_table);
		}
		
		return $data;
	}
	
	/**
	 * Fetches the first column of the first row of the SQL result.
	 *
	 * @return string
	 */
	public function fetchOne()
	{
		$result = $this->_adapter->newStatement($this, false)->getResult();
		$row = $result->fetch_row();
		$result->close();
		return $row === null ? null : $row[0];
	}
	
	/**
	 * Tests query to determine if expressions or aliases columns exist.
	 *
	 * @return boolean
	 */
	public function isReadOnly()
	{
		$readOnly = false;
		$fields   = $this->_parts[Select::COLUMNS];
	
		if (!count($fields)) {
			return $readOnly;
		}
	
		foreach ($fields as $columnEntry) {
			$column = $columnEntry[1];
			$alias = $columnEntry[2];
	
			if ($alias !== null) {
				$column = $alias;
			}
	
			switch (true) {
				case ($column == self::SQL_WILDCARD):
					break;
	
				case ($column instanceof Expr):
					$readOnly = true;
					break 2;
			}
		}
	
		return $readOnly;
	}
}
