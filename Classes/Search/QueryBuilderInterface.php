<?php
declare(strict_types=1);

namespace Flowpack\SimpleSearch\Search;

/**
 * Query Builder Interface for searches, similar to Neos\ContentRepository\Search\Search\QueryBuilderInterface
 * but not dealing with CR nodes
 */
interface QueryBuilderInterface
{
    /**
     * Sort descending by $propertyName
     *
     * @param string $propertyName the property name to sort by
     * @return QueryBuilderInterface
     */
    public function sortDesc(string $propertyName): QueryBuilderInterface;

    /**
     * Sort ascending by $propertyName
     *
     * @param string $propertyName the property name to sort by
     * @return QueryBuilderInterface
     */
    public function sortAsc(string $propertyName): QueryBuilderInterface;

    /**
     * output only $limit records
     *
     * @param integer $limit
     * @return QueryBuilderInterface
     */
    public function limit(?int $limit): QueryBuilderInterface;

    /**
     * Start returned results $from number results.
     *
     * @param integer $from
     * @return QueryBuilderInterface
     */
    public function from(?int $from): QueryBuilderInterface;

    /**
     * add an exact-match query for a given property
     *
     * @param string $propertyName
     * @param mixed $propertyValue
     * @return QueryBuilderInterface
     */
    public function exactMatch(string $propertyName, $propertyValue): QueryBuilderInterface;

    /**
     * add an like query for a given property
     *
     * @param string $propertyName
     * @param $propertyValue
     * @return QueryBuilderInterface
     */
    public function like(string $propertyName, $propertyValue): QueryBuilderInterface;

    /**
     * Match the searchword against the fulltext index
     *
     * @param string $searchword
     * @return QueryBuilderInterface
     */
    public function fulltext(string $searchword): QueryBuilderInterface;

    /**
     * Execute the query and return the list of nodes as result
     *
     * @return array<\Neos\ContentRepository\Domain\Model\NodeInterface>
     */
    public function execute(): array;

    /**
     * Return the total number of hits for the query.
     *
     * @return integer
     */
    public function count(): int;
}
