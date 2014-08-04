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
	 * @param array $fullText array to push to fulltext index for this entry (keys are h1,h2,h3,h4,h5,h6,text) - all keys optional, results weighted by key
	 * @return void
	 */
	public function indexData($identifier, $properties, $fullText) {
		$this->adjustIndexToGivenProperties(array_keys($properties));
		$this->insertOrUpdatePropertiesToIndex($properties, $identifier);
		$this->insertOrUpdateFulltextToIndex($fullText, $identifier);
	}

	/**
	 * @param string $identifier
	 * @return void
	 */
	public function removeData($identifier) {
		$this->connection->query('DELETE FROM objects WHERE __identifier__ = "' . $identifier . '"');
		$this->connection->query('DELETE FROM fulltext WHERE __identifier__ = "' . $identifier . '"');
	}

	/**
	 * @param array $properties
	 * @param string $identifier
	 * @return void
	 */
	protected function insertOrUpdatePropertiesToIndex($properties, $identifier) {
		$propertyColumnNamesString = '__identifier__, ';
		$valueNamesString = ':__identifier__, ';
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
			if (is_array($propertyValue)) {
				$propertyValue = implode(',', $propertyValue);
			}
			$preparedStatement->bindValue($this->preparedStatementArgumentName($statementArgumentNumber), $propertyValue);
			$statementArgumentNumber++;
		}

		$preparedStatement->bindValue(':__identifier__', $identifier);

		$preparedStatement->execute();
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
	 */
	protected function insertOrUpdateFulltextToIndex($fulltext, $identifier) {
		$preparedStatement = $this->connection->prepare('INSERT OR REPLACE INTO fulltext (__identifier__, h1, h2, h3, h4, h5, h6, text) VALUES (:identifier, :h1, :h2, :h3, :h4, :h5, :h6, :text);');
		$preparedStatement->bindValue(':identifier', $identifier);
		$preparedStatement->bindValue(':h1', isset($fulltext['h1']) ? $fulltext['h1'] : '');
		$preparedStatement->bindValue(':h2', isset($fulltext['h2']) ? $fulltext['h2'] : '');
		$preparedStatement->bindValue(':h3', isset($fulltext['h3']) ? $fulltext['h3'] : '');
		$preparedStatement->bindValue(':h4', isset($fulltext['h4']) ? $fulltext['h4'] : '');
		$preparedStatement->bindValue(':h5', isset($fulltext['h5']) ? $fulltext['h5'] : '');
		$preparedStatement->bindValue(':h6', isset($fulltext['h6']) ? $fulltext['h6'] : '');
		$preparedStatement->bindValue(':text', isset($fulltext['text']) ? $fulltext['text'] : '');
		$preparedStatement->execute();
	}

	/**
	 * @param array $fulltext
	 * @param string $identifier
	 */
	public function addToFulltext($fulltext, $identifier) {
		$preparedStatement = $this->connection->prepare('UPDATE OR IGNORE fulltext SET h1 = (h1 || " " || :h1), h2 = (h2 || " " || :h2), h3 = (h3 || " " || :h3), h4 = (h4 || " " || :h4), h5 = (h5 || " " || :h5), h6 = (h6 || " " || :h6), text = (text || " " || :text) WHERE __identifier__ = :identifier;');
		$preparedStatement->bindValue(':identifier', $identifier);
		$preparedStatement->bindValue(':h1', isset($fulltext['h1']) ? $fulltext['h1'] : '');
		$preparedStatement->bindValue(':h2', isset($fulltext['h2']) ? $fulltext['h2'] : '');
		$preparedStatement->bindValue(':h3', isset($fulltext['h3']) ? $fulltext['h3'] : '');
		$preparedStatement->bindValue(':h4', isset($fulltext['h4']) ? $fulltext['h4'] : '');
		$preparedStatement->bindValue(':h5', isset($fulltext['h5']) ? $fulltext['h5'] : '');
		$preparedStatement->bindValue(':h6', isset($fulltext['h6']) ? $fulltext['h6'] : '');
		$preparedStatement->bindValue(':text', isset($fulltext['text']) ? $fulltext['text'] : '');
		$preparedStatement->execute();
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
	 * completely empties the index.
	 */
	public function flush() {
		$this->connection->exec('DROP TABLE objects;');
		$this->connection->exec('DROP TABLE fulltext;');
		$this->createIndexTables();
	}

	/**
	 * Optimize the sqlite database.
	 */
	public function optimize() {
		$this->connection->exec('VACUUM');
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
			h1,
			h2,
			h3,
			h4,
			h5,
			h6,
			text
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