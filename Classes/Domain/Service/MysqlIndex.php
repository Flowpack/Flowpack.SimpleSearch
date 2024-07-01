<?php
declare(strict_types=1);

namespace Flowpack\SimpleSearch\Domain\Service;

use Flowpack\SimpleSearch\Exception;
use Neos\Flow\Annotations as Flow;

/**
 * The MysqlIndex class provides an index using MySQL and its FULLTEXT indexing feature
 */
class MysqlIndex implements IndexInterface
{
    /**
     * @var \PDO
     */
    protected $connection;

    /**
     * @var string
     */
    protected $dataSourceName = '';

    /**
     * @var string
     */
    protected $username;

    /**
     * @var string
     */
    protected $password;

    /**
     * @var string
     */
    protected $pdoDriver;

    /**
     * @var string
     */
    protected $indexName;

    /**
     * Index of fields created for distinct properties of the indexed object
     *
     * @var array<string>
     */
    protected $propertyFieldsAvailable;

    /**
     * @param string $indexName
     * @param string $dataSourceName
     * @Flow\Autowiring(false)
     */
    public function __construct(string $indexName, string $dataSourceName)
    {
        $this->indexName = $indexName;
        $this->dataSourceName = $dataSourceName;
    }

    /**
     * Lifecycle method
     *
     * @throws Exception
     */
    public function initializeObject(): void
    {
        $this->connect();
    }

    /**
     * Connect to the database
     *
     * @return void
     * @throws Exception if the connection cannot be established
     */
    protected function connect(): void
    {
        if ($this->connection !== null) {
            return;
        }

        $splitdsn = explode(':', $this->dataSourceName, 2);
        $this->pdoDriver = $splitdsn[0];

        try {
            $this->connection = new \PDO($this->dataSourceName, $this->username, $this->password);
            $this->connection->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);

            if ($this->pdoDriver === 'mysql') {
                $this->connection->exec('SET SESSION sql_mode=\'ANSI\';');
            }
        } catch (\PDOException $exception) {
            throw new Exception(sprintf('Could not connect to index database with DSN "%s". PDO error: %s', $this->dataSourceName, $exception->getMessage()), 1576771168, $exception);
        }

        $this->createIndexTables();
        $this->loadAvailablePropertyFields();
    }

    /**
     * @param string $identifier identifier for the data
     * @param array $properties Properties to put into index
     * @param array $fullText array to push to fulltext index for this entry (keys are h1,h2,h3,h4,h5,h6,text) - all keys optional, results weighted by key
     * @return void
     */
    public function indexData(string $identifier, array $properties, array $fullText): void
    {
        $this->connection->exec('BEGIN');
        $this->adjustIndexToGivenProperties(array_keys($properties));
        $this->insertOrUpdatePropertiesToIndex($properties, $identifier);
        $this->insertOrUpdateFulltextToIndex($fullText, $identifier);
        $this->connection->exec('COMMIT');
    }

    /**
     * @param string $identifier
     * @return void
     */
    public function removeData(string $identifier): void
    {
        $this->connection->exec('BEGIN');
        $statement = $this->connection->prepare('DELETE FROM "fulltext_objects" WHERE "__identifier__" = :identifier');
        $statement->bindValue(':identifier', $identifier);
        $statement->execute();
        $statement = $this->connection->prepare('DELETE FROM "fulltext_index" WHERE "__identifier__" = :identifier');
        $statement->bindValue(':identifier', $identifier);
        $statement->execute();
        $this->connection->exec('COMMIT');
    }

    /**
     * @param array $properties
     * @param string $identifier
     * @return void
     */
    public function insertOrUpdatePropertiesToIndex(array $properties, string $identifier): void
    {
        $propertyColumnNamesString = '"__identifier__", ';
        $valueNamesString = ':__identifier__, ';
        $statementArgumentNumber = 1;
        foreach ($properties as $propertyName => $propertyValue) {
            $propertyColumnNamesString .= '"' . $propertyName . '", ';
            $valueNamesString .= $this->preparedStatementArgumentName($statementArgumentNumber) . ', ';
            $statementArgumentNumber++;
        }
        $propertyColumnNamesString = trim($propertyColumnNamesString, ", \t\n\r\0\x0B");
        $valueNamesString = trim($valueNamesString, ", \t\n\r\0\x0B");
        $preparedStatement = $this->connection->prepare('REPLACE INTO "fulltext_objects" (' . $propertyColumnNamesString . ') VALUES (' . $valueNamesString . ')');

        $preparedStatement->bindValue(':__identifier__', $identifier);

        $statementArgumentNumber = 1;
        foreach ($properties as $propertyValue) {
            if (is_array($propertyValue)) {
                $propertyValue = implode(',', $propertyValue);
            }
            $preparedStatement->bindValue($this->preparedStatementArgumentName($statementArgumentNumber), $propertyValue);
            $statementArgumentNumber++;
        }

        $preparedStatement->execute();
    }

    /**
     * @param int $argumentNumber
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
    protected function insertOrUpdateFulltextToIndex(array $fulltext, string $identifier): void
    {
        $preparedStatement = $this->connection->prepare('REPLACE INTO "fulltext_index" ("__identifier__", "h1", "h2", "h3", "h4", "h5", "h6", "text") VALUES (:identifier, :h1, :h2, :h3, :h4, :h5, :h6, :text);');
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
        $preparedStatement = $this->connection->prepare('UPDATE IGNORE "fulltext_index" SET "h1" = CONCAT("h1", \' \', :h1), "h2" = CONCAT("h2", \' \', :h2), "h3" = CONCAT("h3", \' \', :h3), "h4" = CONCAT("h4", \' \', :h4), "h5" = CONCAT("h5", \' \', :h5), "h6" = CONCAT("h6", \' \', :h6), "text" = CONCAT("text", \' \', :text) WHERE "__identifier__" = :identifier');
        $preparedStatement->bindValue(':identifier', $identifier);
        $this->bindFulltextParametersToStatement($preparedStatement, $fulltext);
        $preparedStatement->execute();
    }

    /**
     * Binds fulltext parameters to a prepared statement as this happens in multiple places.
     *
     * @param \PDOStatement $preparedStatement
     * @param array $fulltext array (keys are h1,h2,h3,h4,h5,h6,text) - all keys optional
     */
    protected function bindFulltextParametersToStatement(\PDOStatement $preparedStatement, array $fulltext): void
    {
        $preparedStatement->bindValue(':h1', $fulltext['h1'] ?? '');
        $preparedStatement->bindValue(':h2', $fulltext['h2'] ?? '');
        $preparedStatement->bindValue(':h3', $fulltext['h3'] ?? '');
        $preparedStatement->bindValue(':h4', $fulltext['h4'] ?? '');
        $preparedStatement->bindValue(':h5', $fulltext['h5'] ?? '');
        $preparedStatement->bindValue(':h6', $fulltext['h6'] ?? '');
        $preparedStatement->bindValue(':text', $fulltext['text'] ?? '');
    }

    /**
     * Returns an index entry by identifier or NULL if it doesn't exist.
     *
     * @param string $identifier
     * @return array|FALSE
     */
    public function findOneByIdentifier(string $identifier)
    {
        $statement = $this->connection->prepare('SELECT * FROM "fulltext_objects" WHERE "__identifier__" = :identifier LIMIT 1');
        $statement->bindValue(':identifier', $identifier);

        if ($statement->execute()) {
            return $statement->fetch(\PDO::FETCH_ASSOC);
        }

        return false;
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

        if ($statement->execute()) {
            return $statement->fetchAll(\PDO::FETCH_ASSOC);
        }

        return [];
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
        $this->connection->exec('DROP TABLE "fulltext_objects"');
        $this->connection->exec('DROP TABLE "fulltext_index"');
        $this->createIndexTables();
    }

    /**
     * Optimize the database tables.
     *
     * @noinspection PdoApiUsageInspection query MUST be used for OPTIMIZE TABLE to work
     */
    public function optimize(): void
    {
        $this->connection->exec('SET GLOBAL innodb_optimize_fulltext_only = 1');
        $this->connection->query('OPTIMIZE TABLE "fulltext_index"');
        $this->connection->exec('SET GLOBAL innodb_optimize_fulltext_only = 0');
        $this->connection->query('OPTIMIZE TABLE "fulltext_objects", "fulltext_index"');
    }

    /**
     * @return void
     */
    protected function createIndexTables(): void
    {
        $result = $this->connection->query('SHOW TABLES');
        $tables = $result->fetchAll(\PDO::FETCH_COLUMN);

        if (!in_array('fulltext_objects', $tables, true)) {
            $this->connection->exec('CREATE TABLE "fulltext_objects" (
                "__identifier__" VARCHAR(40),
                PRIMARY KEY ("__identifier__")
            ) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci ENGINE = InnoDB');
            $this->propertyFieldsAvailable = [];
        }

        if (!in_array('fulltext_index', $tables, true)) {
            $this->connection->exec('CREATE TABLE "fulltext_index" (
                "__identifier__" VARCHAR(40),
                "h1" MEDIUMTEXT,
                "h2" MEDIUMTEXT,
                "h3" MEDIUMTEXT,
                "h4" MEDIUMTEXT,
                "h5" MEDIUMTEXT,
                "h6" MEDIUMTEXT,
                "text" MEDIUMTEXT,
                PRIMARY KEY ("__identifier__"),
                FULLTEXT nodeindex ("h1",
                "h2",
                "h3",
                "h4",
                "h5",
                "h6",
                "text")
                ) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci ENGINE = InnoDB'
            );
        }
    }

    /**
     * @return void
     */
    protected function loadAvailablePropertyFields(): void
    {
        $result = $this->connection->query('DESCRIBE fulltext_objects');
        $this->propertyFieldsAvailable = $result->fetchAll(\PDO::FETCH_COLUMN);
    }

    /**
     * @param string $propertyName
     */
    protected function addPropertyToIndex(string $propertyName): void
    {
        $this->connection->exec('ALTER TABLE "fulltext_objects" ADD COLUMN "' . $propertyName . '" MEDIUMTEXT DEFAULT NULL');
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
