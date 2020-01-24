<?php
declare(strict_types=1);

namespace Flowpack\SimpleSearch\Domain\Service;

use Neos\Flow\Annotations as Flow;

/**
 * The SqLiteIndex class provides an index using SQLite and its fts3 fulltext indexing feature
 */
class SqLiteIndex implements IndexInterface
{
    /**
     * The storage folder for the index.
     * Should be a directory path ending with a slash.
     *
     * @var string
     */
    protected $storageFolder;

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
     * @param string $storageFolder The absolute file path (with trailing slash) to store this index in.
     * @Flow\Autowiring(false)
     */
    public function __construct(string $indexName, string $storageFolder)
    {
        $this->indexName = $indexName;
        $this->storageFolder = $storageFolder;
    }

    /**
     * Lifecycle method
     */
    public function initializeObject(): void
    {
        $databaseFilePath = $this->storageFolder . md5($this->getIndexName()) . '.db';
        $createDatabaseTables = false;

        if (!is_file($databaseFilePath)) {
            if (!is_dir($this->storageFolder) && !mkdir($concurrentDirectory = $this->storageFolder, 0777, true) && !is_dir($concurrentDirectory)) {
                throw new \RuntimeException(sprintf('Directory "%s" could not be created', $concurrentDirectory), 1576769055);
            }
            $createDatabaseTables = true;
        }
        $this->connection = new \SQLite3($databaseFilePath);

        if ($createDatabaseTables) {
            $this->createIndexTables();
        } else {
            $this->loadAvailablePropertyFields();
        }
    }

    /**
     * @param string $identifier identifier for the data
     * @param array $properties Properties to put into index
     * @param array $fullText array to push to fulltext index for this entry (keys are h1,h2,h3,h4,h5,h6,text) - all keys optional, results weighted by key
     * @return void
     */
    public function indexData(string $identifier, array $properties, array $fullText): void
    {
        $this->connection->query('BEGIN IMMEDIATE TRANSACTION;');
        $this->adjustIndexToGivenProperties(array_keys($properties));
        $this->insertOrUpdatePropertiesToIndex($properties, $identifier);
        $this->insertOrUpdateFulltextToIndex($fullText, $identifier);
        $this->connection->query('COMMIT TRANSACTION;');
    }

    /**
     * @param string $identifier
     * @return void
     */
    public function removeData(string $identifier): void
    {
        $statement = $this->connection->prepare('DELETE FROM objects WHERE __identifier__ = :identifier;');
        $statement->bindValue(':identifier', $identifier);
        $statement->execute();
        $statement = $this->connection->prepare('DELETE FROM fulltext WHERE __identifier__ = :identifier;');
        $statement->bindValue(':identifier', $identifier);
        $statement->execute();
    }

    /**
     * @param array $properties
     * @param string $identifier
     * @return void
     */
    public function insertOrUpdatePropertiesToIndex(array $properties, string $identifier): void
    {
        $propertyColumnNamesString = '__identifier__, ';
        $valueNamesString = ':__identifier__, ';
        $statementArgumentNumber = 1;
        foreach ($properties as $propertyName => $propertyValue) {
            $propertyColumnNamesString .= '"' . $propertyName . '", ';
            $valueNamesString .= $this->preparedStatementArgumentName($statementArgumentNumber) . ', ';
            $statementArgumentNumber++;
        }
        $propertyColumnNamesString = trim($propertyColumnNamesString, ", \t\n\r\0\x0B");
        $valueNamesString = trim($valueNamesString, ", \t\n\r\0\x0B");
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
    protected function preparedStatementArgumentName(int $argumentNumber): string
    {
        return ':arg' . $argumentNumber;
    }

    /**
     * @param array $fulltext
     * @param string $identifier
     */
    protected function insertOrUpdateFulltextToIndex($fulltext, $identifier): void
    {
        $preparedStatement = $this->connection->prepare('INSERT OR REPLACE INTO fulltext (__identifier__, h1, h2, h3, h4, h5, h6, text) VALUES (:identifier, :h1, :h2, :h3, :h4, :h5, :h6, :text);');
        $preparedStatement->bindValue(':identifier', $identifier);
        $this->bindFulltextParametersToStatement($preparedStatement, $fulltext);
        $preparedStatement->execute();
    }

    /**
     * @param array $fulltext
     * @param string $identifier
     */
    public function addToFulltext(array $fulltext, string $identifier): void
    {
        $preparedStatement = $this->connection->prepare('UPDATE OR IGNORE fulltext SET h1 = (h1 || " " || :h1), h2 = (h2 || " " || :h2), h3 = (h3 || " " || :h3), h4 = (h4 || " " || :h4), h5 = (h5 || " " || :h5), h6 = (h6 || " " || :h6), text = (text || " " || :text) WHERE __identifier__ = :identifier;');
        $preparedStatement->bindValue(':identifier', $identifier);
        $this->bindFulltextParametersToStatement($preparedStatement, $fulltext);
        $preparedStatement->execute();
    }

    /**
     * Binds fulltext parameters to a prepared statement as this happens in multiple places.
     *
     * @param \SQLite3Stmt $preparedStatement
     * @param array $fulltext array (keys are h1,h2,h3,h4,h5,h6,text) - all keys optional
     */
    protected function bindFulltextParametersToStatement(\SQLite3Stmt $preparedStatement, array $fulltext): void
    {
        foreach (['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'text'] as $bucketName) {
            $preparedStatement->bindValue(':' . $bucketName, $fulltext[$bucketName] ?? '');
        }
    }

    /**
     * Returns an index entry by identifier or NULL if it doesn't exist.
     *
     * @param string $identifier
     * @return array|FALSE
     */
    public function findOneByIdentifier(string $identifier)
    {
        $statement = $this->connection->prepare('SELECT * FROM objects WHERE __identifier__ = :identifier LIMIT 1');
        $statement->bindValue(':identifier', $identifier);

        return $statement->execute()->fetchArray(SQLITE3_ASSOC);
    }

    /**
     * Execute a prepared statement.
     *
     * @param string $statementQuery The statement query
     * @param array $parameters The statement parameters as map
     * @return array
     */
    public function executeStatement(string $statementQuery, array $parameters): array
    {
        $statement = $this->connection->prepare($statementQuery);
        foreach ($parameters as $parameterName => $parameterValue) {
            $statement->bindValue($parameterName, $parameterValue);
        }

        $result = $statement->execute();
        $resultArray = [];
        while ($resultRow = $result->fetchArray(SQLITE3_ASSOC)) {
            $resultArray[] = $resultRow;
        }

        return $resultArray;
    }

    /**
     * @return string
     */
    public function getIndexName(): string
    {
        return $this->indexName;
    }

    /**
     * completely empties the index.
     */
    public function flush(): void
    {
        $this->connection->exec('DROP TABLE objects;');
        $this->connection->exec('DROP TABLE fulltext;');
        $this->createIndexTables();
    }

    /**
     * Optimize the sqlite database.
     */
    public function optimize(): void
    {
        $this->connection->exec('VACUUM');
    }

    /**
     * @return void
     */
    protected function createIndexTables(): void
    {
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

        $this->propertyFieldsAvailable = [];
    }

    /**
     * @return void
     */
    protected function loadAvailablePropertyFields(): void
    {
        $result = $this->connection->query('PRAGMA table_info(objects);');
        while ($property = $result->fetchArray(SQLITE3_ASSOC)) {
            $this->propertyFieldsAvailable[] = $property['name'];
        }
    }

    /**
     * @param string $propertyName
     */
    protected function addPropertyToIndex(string $propertyName): void
    {
        $this->connection->exec('ALTER TABLE objects ADD COLUMN "' . $propertyName . '";');
        $this->propertyFieldsAvailable[] = $propertyName;
    }

    /**
     * @param array $propertyNames
     * @return void
     */
    protected function adjustIndexToGivenProperties(array $propertyNames): void
    {
        foreach ($propertyNames as $propertyName) {
            if (!in_array($propertyName, $this->propertyFieldsAvailable, true)) {
                $this->addPropertyToIndex($propertyName);
            }
        }
    }
}
