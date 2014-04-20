<?php
namespace Flowpack\SimpleSearch\Domain\Service;

/**
 * Class SqLiteIndex
 *
 * @package Flowpack\SimpleSearch\Domain\Service
 */
class SqLiteIndex implements IndexInterface {

	/**
	 * @var string
	 */
	protected $indexName;

	/**
	 * @var \SQLite3
	 */
	protected $connection;

	/**
	 * Index of fields created for distinct properties of the indexed object
	 *
	 * @var array<string>
	 */
	protected $propertyFieldsAvailable;

	/**
	 * @param string $indexName
	 */
	public function __construct($indexName) {
		$this->indexName = $indexName;
	}

	/**
	 * Lifecycle method
	 */
	public function initializeObject() {
		$databaseFileName = FLOW_PATH_DATA . 'Persistent/Flowpack_SimpleSearch_SqLite/' . md5($this->getIndexName()) . '.db';
		$createDatabaseTables = FALSE;

		if (!file_exists($databaseFileName)) {
			\TYPO3\Flow\Utility\Files::createDirectoryRecursively(FLOW_PATH_DATA . 'Persistent/Flowpack_SimpleSearch_SqLite');
			$createDatabaseTables = TRUE;
		}
		$this->connection = new \SQLite3(FLOW_PATH_DATA . 'Persistent/Flowpack_SimpleSearch_SqLite/' . md5($this->getIndexName()) . '.db');

		if ($createDatabaseTables) {
			$this->createIndexTables();
		} else {
			$this->loadAvailablePropertyFields();
		}
	}

	/**
	 * @param $identifier identifier for the data
	 * @param array $properties Properties to put into index
	 * @param string $fullText string to push to fulltext index for this property
	 * @return void
	 */
	public function indexData($identifier, $properties, $fullText) {
		$this->adjustIndexToGivenProperties(array_keys($properties));
		$existingRowid = $this->fetchPossiblityExistingEntry($identifier);

		$existingRowid = $this->insertOrUpdatePropertiesToIndex($properties, $existingRowid);
		$this->insertOrUpdateFulltextToIndex($fullText, $identifier, $existingRowid);
	}

	/**
	 * @param string $identifier
	 * @return void
	 */
	public function removeData($identifier) {
		$rowid = $this->fetchPossiblityExistingEntry($identifier);
		if ($rowid !== NULL) {
			$this->connection->query('DELETE FROM objects WHERE rowid = ' . $rowid);
			$this->connection->query('DELETE FROM fulltext WHERE rowid = ' . $rowid);
		}
	}

	/**
	 * @param string $identifier
	 * @return integer
	 */
	protected function fetchPossiblityExistingEntry($identifier) {
		$preparedStatement = $this->connection->prepare('SELECT rowid FROM objects WHERE __identifier__ = :identifier;');
		$preparedStatement->bindValue(':identifier', $identifier);

		$result = $preparedStatement->execute();
		if ($result->numColumns() && $result->columnType(0) != SQLITE3_NULL) {
			$resultRow = $result->fetchArray(SQLITE3_ASSOC);
			if (isset($resultRow['rowid'])) {
				return $result['rowid'];
			}
		}

		return NULL;
	}

	/**
	 * @param array $properties
	 * @param integer $rowid
	 * @return integer rowid of inserted row
	 */
	protected function insertOrUpdatePropertiesToIndex($properties, $rowid = NULL) {
		$propertyColumnNamesString = ($rowid !== NULL ? 'rowid, ' : '');
		$valueNamesString = ($rowid !== NULL ? ':rowid, ' : '');
		$statementArgumentNumber = 1;
		foreach ($properties as $propertyName => $propertyValue) {
			$propertyColumnNamesString .= $propertyName . ', ';
			$valueNamesString .= $this->preparedStatementArgumentName($statementArgumentNumber) . ' , ';
			$statementArgumentNumber++;
		}
		$propertyColumnNamesString = trim($propertyColumnNamesString);
		$propertyColumnNamesString = trim($propertyColumnNamesString, ',');
		$valueNamesString = trim($valueNamesString);
		$valueNamesString = trim($valueNamesString, ',');
		$preparedStatement = $this->connection->prepare('INSERT OR REPLACE INTO objects (' . $propertyColumnNamesString . ') VALUES (' . $valueNamesString . ');');

		$statementArgumentNumber = 1;
		foreach ($properties as $propertyValue) {
			$preparedStatement->bindValue($this->preparedStatementArgumentName($statementArgumentNumber), $propertyValue);
			$statementArgumentNumber++;
		}
		if ($rowid !== NULL) {
			$preparedStatement->bindValue(':rowid', $rowid);
		}

		$preparedStatement->execute();
		return $this->connection->lastInsertRowID();
	}

	/**
	 * @param integer $argumentNumber
	 * @return string
	 */
	protected function preparedStatementArgumentName($argumentNumber) {
		return ':arg' . $argumentNumber;
	}

	/**
	 * @param string $fulltext
	 * @param string $identifier
	 * @param integer $rowid
	 */
	protected function insertOrUpdateFulltextToIndex($fulltext, $identifier, $rowid) {
		$preparedStatement = $this->connection->prepare('INSERT OR REPLACE INTO fulltext (rowid, __identifier__, content) VALUES (:rowid, :identifier, :content);');
		$preparedStatement->bindValue(':rowid', $rowid);
		$preparedStatement->bindValue(':identifier', $identifier);
		$preparedStatement->bindValue(':content', $fulltext);
	}

	/**
	 * @param string $query
	 * @return array
	 */
	public function query($query) {
		$result = $this->connection->query($query);
		$resultArray = array();
		while ($resultRow = $result->fetchArray(SQLITE3_ASSOC)) {
			$resultArray[] = $resultRow;
		}

		return $resultArray;
	}

	/**
	 * @return string
	 */
	public function getIndexName() {
		return $this->indexName;
	}

	/**
	 *
	 */
	public function flush() {
		$this->connection->exec('TRUNCATE TABLE objects;');
		$this->connection->exec('TRUNCATE TABLE fulltext;');
	}

	/**
	 * @return void
	 */
	protected function createIndexTables() {
		$this->connection->exec('CREATE TABLE objects (
			__identifier__ VARCHAR,
			PRIMARY KEY ("__identifier__")
		);');

		$this->connection->exec('CREATE VIRTUAL TABLE fulltext USING fts3(
			__identifier__ VARCHAR,
			content TEXT
		);');

		$this->propertyFieldsAvailable = array();
	}

	/**
	 * @return void
	 */
	protected function loadAvailablePropertyFields() {
		$result = $this->connection->query('PRAGMA table_info(objects);');
		while ($property = $result->fetchArray(SQLITE3_ASSOC)) {
			$this->propertyFieldsAvailable[] = $property['name'];
		}
	}

	/**
	 * @param string $propertyName
	 */
	protected function addPropertyToIndex($propertyName) {
		$this->connection->exec('ALTER TABLE objects ADD COLUMN ' . $propertyName . ';');
		$this->propertyFieldsAvailable[] = $propertyName;
	}

	/**
	 * @param array $propertyNames
	 * @return void
	 */
	protected function adjustIndexToGivenProperties(array $propertyNames) {
		foreach ($propertyNames as $propertyName) {
			if (!in_array($propertyName, $this->propertyFieldsAvailable)) {
				$this->addPropertyToIndex($propertyName);
			}
		}
	}
}