<?php
namespace MDO;

class Table{
	const ADAPTER		  = 'db';
	const SCHEMA		   = 'schema';
	const NAME			 = 'name';
	const PRIMARY		  = 'primary';
	const SEQUENCE		 = 'sequence';
	const ROW_CLASS		 = 'rowClass';
	
	const SELECT_WITH_FROM_PART	= true;
	const SELECT_WITHOUT_FROM_PART = false;
	
	/**
	 * 
	 * @var Adapter
	 */
	protected static $_defaultAdapter;

	/**
	 * Adapter object.
	 *
	 * @var Adapter
	 */
	protected $_db;

	/**
	 * The schema name (default null means current schema)
	 *
	 * @var array
	 */
	protected $_schema = null;

	/**
	 * The table name.
	 *
	 * @var string
	 */
	protected $_name = null;

	/**
	 * The primary key column or columns.
	 * A compound key should be declared as an array.
	 * You may declare a single-column primary key
	 * as a string.
	 * modified by shen2 必须是数组!
	 *
	 * @var array 
	 */
	protected $_primary = null;

	/**
	 * If your primary key is a compound key, and one of the columns uses
	 * an auto-increment or sequence-generated value, set _identity
	 * to the ordinal index in the $_primary array for that column.
	 * Note this index is the position of the column in the primary key,
	 * not the position of the column in the table.  The primary key
	 * array is 0-based.
	 *
	 * @var integer
	 */
	protected $_identity = 0;

	/**
	 * Define the logic for new values in the primary key.
	 * May be a string, boolean true, or boolean false.
	 *
	 * @var mixed
	 */
	protected $_sequence = true;

	protected $_defaultValues = [];

	protected $_rowClass = DataObject::class;

	/**
	 * Constructor.
	 *
	 * Supported params for $config are:
	 * - db			  = user-supplied instance of database connector,
	 *					 or key name of registry instance.
	 * - name			= table name.
	 * - primary		 = string or array of primary key(s).
	 *
	 * @param  mixed $config Array of user-specified config options, or just the Db Adapter.
	 * @return void
	 */
	public function __construct(array $config = [])
	{
		if ($config) {
			$this->setOptions($config);
		}

		$this->_setup();
	}

	/**
	 * setOptions()
	 *
	 * @param array $options
	 * @return void
	 */
	public function setOptions(Array $options)
	{
		if (isset($options[self::ADAPTER]))
			$this->_setAdapter($options[self::ADAPTER]);
		if (isset($options[self::SCHEMA]))
			$this->_schema = (string) $options[self::SCHEMA];
		if (isset($options[self::NAME]))
			$this->_name = (string) $options[self::NAME];
		if (isset($options[self::PRIMARY]))
			$this->_primary = (array) $options[self::PRIMARY];
		if (isset($options[self::SEQUENCE]))
			$this->_sequence = $options[self::SEQUENCE];
		if (isset($options[self::ROW_CLASS]))
		    $this->_rowClass = $options[self::ROW_CLASS];
	}
	
	/**
	 * set the default values for the table class
	 *
	 * @param array $defaultValues
	 */
	public function setDefaultValues(Array $defaultValues)
	{
		$this->_defaultValues = array_merge($this->_defaultValues, $defaultValues);
	}

	public function getDefaultValues()
	{
		return $this->_defaultValues;
	}

	/**
	 * Sets the default Adapter for all \MDO\Table objects.
	 *
	 * @param  Adapter $db an Adapter object
	 * @return void
	 */
	public static function setDefaultAdapter(Adapter $db)
	{
	    self::$_defaultAdapter = $db;
	}
	
	/**
	 * Gets the default Adapter for all \MDO\Table objects.
	 *
	 * @return Adapter or null
	 */
	public static function getDefaultAdapter()
	{
	    return self::$_defaultAdapter;
	}
	
	/**
	 * @param  Adapter $db an Adapter object
	 */
	protected function _setAdapter(Adapter $db)
	{
		$this->_db = $db;
	}

	/**
	 * Gets the Adapter for this particular \MDO\Table object.
	 *
	 * @return Adapter
	 */
	public function getAdapter()
	{
		return $this->_db;
	}

	/**
	 * Turnkey for initialization of a table object.
	 * Calls other protected methods for individual tasks, to make it easier
	 * for a subclass to override part of the setup logic.
	 *
	 * @return void
	 */
	protected function _setup()
	{
		$this->_setupDatabaseAdapter();
	}

	/**
	 * Initialize database adapter.
	 *
	 * @return void
	 */
	protected function _setupDatabaseAdapter()
	{
		if (! $this->_db) {
			$this->_db = self::getDefaultAdapter();
			if (!$this->_db instanceof Adapter) {
				throw new DataObjectException('No adapter found for ' . get_called_class());
			}
		}
	}

	/**
	 * Returns table information.
	 *
	 * You can elect to return only a part of this information by supplying its key name,
	 * otherwise all information is returned as an array.
	 *
	 * @param  string $key The specific info part to return OPTIONAL
	 * @return mixed
	 */
	public function info($key = null)
	{
		$info = array(
			self::SCHEMA    => $this->_schema,
			self::NAME 	    => $this->_name,
			self::PRIMARY   => $this->_primary,
			self::SEQUENCE  => $this->_sequence
		);

		if ($key === null) {
			return $info;
		}

		if (!array_key_exists($key, $info)) {
			throw new DataObjectException('There is no table information for the key "' . $key . '"');
		}

		return $info[$key];
	}

	public function getName(){
		return $this->_name;
	}

	public function getPrimary(){
		return $this->_primary;
	}

	public function getRowClass(){
		return $this->_rowClass;
	}

	/**
	 * Returns an instance of a Select object.
	 *
	 * @param bool $withFromPart Whether or not to include the from part of the select based on the table
	 * @return Select
	 */
	public function select($withFromPart = self::SELECT_WITHOUT_FROM_PART)
	{
		$select = new Select($this->_db);
		$select->setTable($this);
		if ($withFromPart == self::SELECT_WITH_FROM_PART) {
			$select->from($this->_name, Select::SQL_WILDCARD, $this->_schema);
		}
		return $select;
	}
	
	/**
	 * Returns an instance of a Select object.
	 *
	 * @param string|array|Expr $columns
	 * @return Select
	 */
	public function selectCol($columns = null)
	{
		$select = new Select($this->_db);
		$select->setTable($this);
		$select->from($this->_name, $columns === null ? Select::SQL_WILDCARD : $columns, $this->_schema);
		return $select;
	}

	/**
	 * Inserts a new row.
	 *
	 * @param  array  $data  Column-value pairs.
	 * @return mixed		 The primary key of the row inserted.
	 */
	public function insert(array $data)
	{
		/**
		 * \MDO\Table assumes that if you have a compound primary key
		 * and one of the columns in the key uses a sequence,
		 * it's the _first_ column in the compound key.
		 */
		$pkIdentity = $this->_primary[$this->_identity];

		/**
		 * If this table uses a database sequence object and the data does not
		 * specify a value, then get the next ID from the sequence and add it
		 * to the row.  We assume that only the first column in a compound
		 * primary key takes a value from a sequence.
		 */
		if (is_string($this->_sequence) && !isset($data[$pkIdentity])) {
			$data[$pkIdentity] = $this->_db->nextSequenceId($this->_sequence);
			$pkSuppliedBySequence = true;
		}

		/**
		 * If the primary key can be generated automatically, and no value was
		 * specified in the user-supplied data, then omit it from the tuple.
		 * 
		 * Note: this checks for sensible values in the supplied primary key
		 * position of the data.  The following values are considered empty:
		 *   null, false, true, '', []
		 */
		if (!isset($pkSuppliedBySequence) && array_key_exists($pkIdentity, $data)) {
			if ($data[$pkIdentity] === null										// null
				|| $data[$pkIdentity] === ''									   // empty string
				|| is_bool($data[$pkIdentity])									 // boolean
				|| (is_array($data[$pkIdentity]) && empty($data[$pkIdentity]))) {  // empty array
				unset($data[$pkIdentity]);
			}
		}

		/**
		 * INSERT the new row.
		 */
		$insertQuery = (new Insert($this->_db))
			->into(($this->_schema ? $this->_schema . '.' : '') . $this->_name)
			->columns(array_keys($data))
			->values(array_values($data));
		$insertQuery->query();

		/**
		 * Fetch the most recent ID generated by an auto-increment
		 * or IDENTITY column, unless the user has specified a value,
		 * overriding the auto-increment mechanism.
		 */
		if ($this->_sequence === true && !isset($data[$pkIdentity])) {
			$data[$pkIdentity] = $this->_db->insert_id;
		}

		/**
		 * Return an associative array of the PK column/value pairs.
		 */
		return array_intersect_key($data, array_flip($this->_primary));
	}
	
	/**
	 * Inserts Delayed a new row.
	 *
	 * @param  array  $data  Column-value pairs.
	 * @return \mysqli_result mysql result
	 */
	public function insertDelayed(array $data)
	{
		return $this->_db->insert('DELAYED')
			->into(($this->_schema ? $this->_schema . '.' : '') . $this->_name)
			->columns(array_keys($data))
			->values(array_values($data))
			->query();
	}
	
	/**
	 * Inserts Ignore a new row.
	 *
	 * @param  array  $data  Column-value pairs.
	 * @return \mysqli_result mysql result
	 */
	public function insertIgnore(array $data)
	{
		return $this->_db->insert('IGNORE')
			->into(($this->_schema ? $this->_schema . '.' : '') . $this->_name)
			->columns(array_keys($data))
			->values(array_values($data))
			->query();
	}
	
	/**
	 * 使用insertOnDuplicateKeyUpdate 插入一条记录，插入后不会refresh
	 * 
	 * @param  array  $data  Column-value pairs.
	 * @return \mysqli_result mysql result
	 */
	public function insertOrUpdateRow(array $data)
	{
		$updateData = array_diff_key($data, array_flip($this->_primary));

		/**
		 * INSERT the new row.
		 */
		$insertQuery = (new Insert($this->_db))
			->into(($this->_schema ? $this->_schema . '.' : '') . $this->_name)
			->columns(array_keys($data))
			->values(array_values($data))
			->onDuplicateKeyUpdate($updateData);
		return $insertQuery->query();
	}
	
	/**
	 * Insert multiple rows.
	 *
	 * @param  array  $data  values
	 * @return \mysqli_result mysql result
	 */
	public function insertMulti(array $data)
	{
	    if (empty($data))
	        return false;
	    
	    $columns = array_keys(current($data));
	    
	    $insertQuery = (new Insert($this->_db))
	       ->into(($this->_schema ? $this->_schema . '.' : '') . $this->_name)
	       ->columns($columns);
	    
	    foreach($data as $row){
	        $values = [];
	        
	        foreach($columns as $col){
	            // Doesn't check isset($row[$col]), because it already cause a Notice in log.
	            $values[] = $row[$col];
	        }
	        
	        $insertQuery->values($values);
	    }
	    
	    return $insertQuery->query();
	}
	
	/**
	 * Updates existing rows.
	 *
	 * @param  array		$data  Column-value pairs.
	 * @param  array|string $where An SQL WHERE clause, or an array of SQL WHERE clauses.
	 * @return \mysqli_result
	 */
	public function update(array $data, $where)
	{
		return (new Update($this->_db))
			->table(($this->_schema ? $this->_schema . '.' : '') . $this->_name)
			->set($data)
			->whereClauses($where)
			->query();
	}

	/**
	 * Deletes existing rows.
	 *
	 * @param  array|string $where SQL WHERE clause(s).
	 * @return \mysqli_result
	 */
	public function delete($where)
	{
		return (new Delete($this->_db))
			->from(($this->_schema ? $this->_schema . '.' : '') . $this->_name)
			->whereClauses($where)
			->query();
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
	public function find()
	{
		$args = func_get_args();
		$keyNames = array_values($this->_primary);

		if (count($args) != count($keyNames)) {
			throw new DataObjectException("Too few or too many columns for the primary key");
		}

		$whereList = [];
		$numberTerms = 0;
		foreach ($args as $keyPosition => $keyValues) {
			$keyValuesCount = count($keyValues);
			// Coerce the values to an array.
			// Don't simply typecast to array, because the values
			// might be Expr objects.
			if (!is_array($keyValues)) {
				$keyValues = array($keyValues);
			}
			if ($numberTerms == 0) {
				$numberTerms = $keyValuesCount;
			} else if ($keyValuesCount != $numberTerms) {
				throw new DataObjecgtException("Missing value(s) for the primary key");
			}
			$keyValues = array_values($keyValues);
			for ($i = 0; $i < $keyValuesCount; ++$i) {
				if (!isset($whereList[$i])) {
					$whereList[$i] = [];
				}
				$whereList[$i][$keyPosition] = $keyValues[$i];
			}
		}
		
		if (count($whereList) === 0) {
			// empty where clause should return empty rowset
			throw new SelectException('Where clause is empty.');
		}

		$whereOrTerms = [];
		$tableName = $this->_db->quoteTableAs($this->_name, null, true);
		foreach ($whereList as $keyValueSets) {
			$whereAndTerms = [];
			foreach ($keyValueSets as $keyPosition => $keyValue) {
				//$type = $this->_metadata[$keyNames[$keyPosition]]['DATA_TYPE'];
				$columnName = $this->_db->quoteIdentifier($keyNames[$keyPosition], true);
				$whereAndTerms[] = $this->_db->quoteInto(
					$tableName . '.' . $columnName . ' = ?',
					$keyValue);
			}
			$whereOrTerms[] = '(' . implode(' AND ', $whereAndTerms) . ')';
		}
		
		return $this->select()
			->where(implode(' OR ', $whereOrTerms))
			->yieldAll();
	}
	
	/**
	 *
	 * @return \SplFixedArray
	 */
	public function fetchAllByPrimary($keys){
		if (empty($keys))
			return new \SplFixedArray(0);
	
		return $this->select()
			->where($this->_db->quoteIdentifier($this->_primary[0], true) . ' in (' . $this->_db->quoteArray($keys) . ')')
			->fetchAll();
	}

	/**
	 * Generate WHERE clause from user-supplied string or array
	 *
	 * @param  string|array $where  OPTIONAL An SQL WHERE clause.
	 * @return Select
	 */
	protected function _where(Select $select, $where)
	{
		$where = (array) $where;

		foreach ($where as $key => $val) {
			// is $key an int?
			if (is_int($key)) {
				// $val is the full condition
				$select->where($val);
			} else {
				// $key is the condition with placeholder,
				// and $val is quoted into the condition
				$select->where($key, $val);
			}
		}

		return $select;
	}
	
}
