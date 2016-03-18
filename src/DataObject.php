<?php
namespace MDO;

class DataObject extends \ArrayObject
{

	/**
	 * Default adapter object.
	 *
	 * @var Adapter
	 */
	protected static $_db;

	/**
	 * The schema name (default null means current schema)
	 *
	 * @var array
	 */
	protected static $_schema = null;

	/**
	 * The table name.
	 *
	 * @var string
	 */
	protected static $_name = null;

	/**
	 * The primary key column or columns.
	 * A compound key should be declared as an array.
	 * You may declare a single-column primary key
	 * as a string.
	 * modified by shen2 必须是数组!
	 *
	 * @var array
	 */
	protected static $_primary = null;

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
	protected static $_identity = 0;

	/**
	 * Define the logic for new values in the primary key.
	 * May be a string, boolean true, or boolean false.
	 *
	 * @var mixed
	 */
	protected static $_sequence = true;

	protected static $_defaultValues = [];

	/**
	 * Fetches a new blank row (not from the database).
	 *
	 * @param  array $data OPTIONAL data to populate in the new row.
	 * @param  string $defaultSource OPTIONAL flag to force default values into new row
	 * @return DataObject
	 */
	public static function createRow(array $data = [])
	{
		$row = new static(static::$_defaultValues, false, false);
		$row->setFromArray($data);
		return $row;
	}

	/*	下面是Object实例		*/
	const ARRAYOBJECT_FLAGS = 0;//ArrayObject::ARRAY_AS_PROPS;

	/**
	 * This is set to a copy of $_data when the data is fetched from
	 * a database, specified as a new tuple in the constructor, or
	 * when dirty data is posted to the database with save().
	 *
	 * @var array
	 */
	protected $_cleanData = array();

	/**
	 * Tracks columns where data has been updated. Allows more specific insert and
	 * update operations.
	 *
	 * @var array
	 */
	protected $_modifiedFields = array();

	/**
	 * Connected is true if we have a reference to a live
	 * \MDO\Table_Abstract object.
	 * This is false after the Rowset has been deserialized.
	 *
	 * @var boolean
	 */
	protected $_connected = true;

	/**
	 * A row is marked read only if it contains columns that are not physically represented within
	 * the database schema (e.g. evaluated columns/Expr columns). This can also be passed
	 * as a run-time config options as a means of protecting row data.
	 *
	 * @var boolean
	 */
	protected $_readOnly = false;
	
	
	/**
	 * Constructor.
	 *
	 * Supported params for $config are:-
	 * - table	   = class name or object of type \MDO\Table_Abstract
	 * - data		= values of columns in this row.
	 *
	 * @param  array $config OPTIONAL Array of user-specified config options.
	 * @param
	 * @return void
	 * @throws DataObjectException
	 */
	/**
	 * 
	 * @param array	  $data
	 * @param boolean $stored
	 * @param boolean $readOnly
	 */
	public function __construct($data = array(), $stored = null, $readOnly = null)
	{
		parent::__construct($data, static::ARRAYOBJECT_FLAGS);
		
		if ($stored === true) {
			$this->_cleanData = $this->getArrayCopy();
		}

		if ($readOnly === true) {
			$this->setReadOnly(true);
		}

		$this->init();
	}

	/**
	 * Set row field value
	 *
	 * @param  string $columnName The column key.
	 * @param  mixed  $value	  The value for the property.
	 * @return void
	 * @throws DataObjectException
	 */
	public function offsetSet($columnName, $value)
	{
		parent::offsetSet($columnName,$value);
		$this->_modifiedFields[$columnName] = true;
	}

	/**
	 * Store table, primary key and data in serialized object
	 *
	 * @return array
	 */
	public function __sleep()
	{
		return array('_cleanData', '_readOnly' ,'_modifiedFields');
	}

	/**
	 * Setup to do on wakeup.
	 * A de-serialized Row should not be assumed to have access to a live
	 * database connection, so set _connected = false.
	 *
	 * @return void
	 */
	public function __wakeup()
	{
		$this->_connected = false;
	}

	/**
	 * Initialize object
	 *
	 * Called from {@link __construct()} as final step of object instantiation.
	 *
	 * @return void
	 */
	public function init()
	{
	}

	/**
	 * Test the connected status of the row.
	 *
	 * @return boolean
	 */
	public function isConnected()
	{
		return $this->_connected;
	}
	
	public function isModified(){
		return !empty($this->_modifiedFields); 
	}

	/**
	 * Test the read-only status of the row.
	 *
	 * @return boolean
	 */
	public function isReadOnly()
	{
		return $this->_readOnly;
	}

	/**
	 * Set the read-only status of the row.
	 *
	 * @param boolean $flag
	 * @return boolean
	 */
	public function setReadOnly($flag)
	{
		$this->_readOnly = (bool) $flag;
	}

	/**
	 * Saves the properties to the database.
	 *
	 * This performs an intelligent insert/update, and reloads the
	 * properties with fresh data from the table on success.
	 *
	 * @return mixed The primary key value(s), as an associative array if the
	 *	 key is compound, or a scalar if the key is single-column.
	 */
	public function save($realRefresh = true)
	{
		/**
		 * If the _cleanData array is empty,
		 * this is an INSERT of a new row.
		 * Otherwise it is an UPDATE.
		 */
		if (empty($this->_cleanData)) {
			return $this->_doInsert($realRefresh);
		} else {
			return $this->_doUpdate($realRefresh);
		}
	}

	/**
	 * Sets all data in the row from an array.
	 *
	 * @param  array $data
	 * @return DataObject Provides a fluent interface
	 */
	public function setFromArray(array $data)
	{
		//原来是array_intersect_key($data, $this->getArrayCopy())，现在取消参数列表检查，因此直接使用data
		foreach ($data as $columnName => $value) {
			$this[$columnName] = $value;
		}

		return $this;
	}

	/**
	 * Refreshes properties from the database.
	 *
	 * @return void
	 */
	public function refresh($real = true)
	{
		return $real ? $this->_realRefresh() : $this->_refresh();
	}
}
