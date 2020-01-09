<?php
declare(strict_types=1);

namespace Flowpack\SimpleSearch\Domain\Service;

/**
 * Interface IndexInterface
 */
interface IndexInterface
{
    /**
     * @param string $identifier identifier for the data
     * @param array $properties Properties to put into index
     * @param array $fullText array to push to fulltext index for this entry (keys are h1,h2,h3,h4,h5,h6,text) - all keys optional, results weighted by key
     * @return void
     */
    public function indexData(string $identifier, array $properties, array $fullText): void;

    /**
     * Use this only to update properties on existing entries. For new entries use "indexData"
     * This method is subject to change and could be removed from the interface at any time.
     *
     * @param array $properties
     * @param string $identifier
     * @return void
     */
    public function insertOrUpdatePropertiesToIndex(array $properties, string $identifier): void;

    /**
     * @param string $identifier
     * @return void
     */
    public function removeData(string $identifier): void;

    /**
     * @return string
     */
    public function getIndexName(): string;

    /**
     * @param array $fulltext
     * @param string $identifier
     * @return void
     */
    public function addToFulltext(array $fulltext, string $identifier): void;

    /**
     * Empty the index.
     *
     * @return void
     */
    public function flush(): void;

    /**
     * Optimize the index, can be a void operation if the index has no such operation.
     *
     * @return void
     */
    public function optimize(): void;

    /**
     * Execute a query statement.
     *
     * @param string $statementQuery
     * @param array $parameters
     * @return array
     */
    public function executeStatement(string $statementQuery, array $parameters): array;
}
