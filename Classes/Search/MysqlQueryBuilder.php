<?php
declare(strict_types=1);

namespace Flowpack\SimpleSearch\Search;

use Flowpack\SimpleSearch\Domain\Service\IndexInterface;

/**
 * Query Builder for searches
 *
 * Note: some signatures are not as strict as in the interfaces, because two query builder interfaces are "mixed"
 * in classes implementing this one
 */
class MysqlQueryBuilder implements QueryBuilderInterface
{
    /**
     * @var IndexInterface
     */
    protected $indexClient;

    /**
     * @var integer
     */
    protected $limit;

    /**
     * @var integer
     */
    protected $from;

    /**
     * Sorting strings
     *
     * @var array<string>
     */
    protected $sorting = [];

    /**
     * where clauses
     *
     * @var array
     */
    protected $where = [];

    /**
     * Map of query parameters to bind to the final statement.
     *
     * @var array
     */
    protected $parameterMap = [];

    /**
     * Injection method used by Flow dependency injection
     *
     * @param IndexInterface $indexClient
     */
    public function injectIndexClient(IndexInterface $indexClient)
    {
        $this->indexClient = $indexClient;
    }

    /**
     * Sort descending by $propertyName
     *
     * @param string $propertyName the property name to sort by
     * @return QueryBuilderInterface
     */
    public function sortDesc($propertyName): QueryBuilderInterface
    {
        $this->sorting[] = '"fulltext_objects"."' . $propertyName . '" DESC';

        return $this;
    }

    /**
     * Sort ascending by $propertyName
     *
     * @param string $propertyName the property name to sort by
     * @return QueryBuilderInterface
     */
    public function sortAsc($propertyName): QueryBuilderInterface
    {
        $this->sorting[] = '"fulltext_objects"."' . $propertyName . '" ASC';

        return $this;
    }

    /**
     * output only $limit records
     *
     * @param integer|null $limit
     * @return QueryBuilderInterface
     */
    public function limit($limit): QueryBuilderInterface
    {
        if ($limit !== null) {
            $limit = (int)$limit;
        }
        $this->limit = $limit === null ? $limit : (int)$limit;
        return $this;
    }

    /**
     * Start returned results $from number results.
     *
     * @param integer|null $from
     * @return QueryBuilderInterface
     */
    public function from($from): QueryBuilderInterface
    {
        $this->from = $from === null ? $from : (int)$from;
        return $this;
    }

    /**
     * add an exact-match query for a given property
     *
     * @param string $propertyName
     * @param string $propertyValue
     * @return QueryBuilderInterface
     */
    public function exactMatch(string $propertyName, $propertyValue): QueryBuilderInterface
    {
        $this->compare($propertyName, $propertyValue, '=');

        return $this;
    }

    /**
     * add an like query for a given property
     *
     * @param string $propertyName
     * @param $propertyValue
     * @return QueryBuilderInterface
     */
    public function like(string $propertyName, $propertyValue): QueryBuilderInterface
    {
        $parameterName = ':' . md5($propertyName . '#' . count($this->where));
        $this->where[] = '("' . $propertyName . '" LIKE ' . $parameterName . ')';
        $this->parameterMap[$parameterName] = '%' . $propertyValue . '%';

        return $this;
    }

    /**
     * @param string $searchword
     * @return QueryBuilderInterface
     */
    public function fulltext($searchword): QueryBuilderInterface
    {
        $parameterName = ':' . md5('FULLTEXT#' . count($this->where));
        $this->where[] = '("__identifier__" IN (SELECT "__identifier__" FROM "fulltext_index" WHERE MATCH ("h1", "h2", "h3", "h4", "h5", "h6", "text") AGAINST (' . $parameterName . ')))';
        $this->parameterMap[$parameterName] = $searchword;

        return $this;
    }

    /**
     * add a greater than query for a given property
     *
     * @param string $propertyName
     * @param string $propertyValue
     * @return QueryBuilderInterface
     */
    public function greaterThan($propertyName, $propertyValue)
    {
        return $this->compare($propertyName, $propertyValue, '>');
    }

    /**
     * add a greater than or equal query for a given property
     *
     * @param string $propertyName
     * @param string $propertyValue
     * @return QueryBuilderInterface
     */
    public function greaterThanOrEqual($propertyName, $propertyValue)
    {
        return $this->compare($propertyName, $propertyValue, '>=');
    }

    /**
     * add a less than query for a given property
     *
     * @param $propertyName
     * @param $propertyValue
     * @return QueryBuilderInterface
     */
    public function lessThan($propertyName, $propertyValue)
    {
        return $this->compare($propertyName, $propertyValue, '<');
    }

    /**
     * add a less than or equal query for a given property
     *
     * @param $propertyName
     * @param $propertyValue
     * @return QueryBuilderInterface
     */
    public function lessThanOrEqual($propertyName, $propertyValue)
    {
        return $this->compare($propertyName, $propertyValue, '<=');
    }

    /**
     * Execute the query and return the list of results
     *
     * @return array
     */
    public function execute(): array
    {
        $query = $this->buildQueryString();
        $result = $this->indexClient->executeStatement($query, $this->parameterMap);

        if (empty($result)) {
            return [];
        }

        return array_values($result);
    }

    /**
     * Return the total number of hits for the query.
     *
     * @return integer
     */
    public function count(): int
    {
        $result = $this->execute();
        return count($result);
    }

    /**
     * Produces a snippet with the first match result for the search term.
     *
     * @param string $searchword The search word
     * @param integer $resultTokens The amount of tokens (words) to get surrounding the match hit. (defaults to 60)
     * @param string $ellipsis added to the end of the string if the text was longer than the snippet produced. (defaults to "...")
     * @param string $beginModifier added immediately before the searchword in the snippet (defaults to <b>)
     * @param string $endModifier added immediately after the searchword in the snippet (defaults to </b>)
     * @return string
     */
    public function fulltextMatchResult($searchword, $resultTokens = 60, $ellipsis = '...', $beginModifier = '<b>', $endModifier = '</b>'): string
    {
        return '';
    }

    /**
     * Match any value in the given array for the property
     *
     * @param string $propertyName
     * @param array $propertyValues
     * @return QueryBuilderInterface
     */
    public function anyMatch(string $propertyName, array $propertyValues): QueryBuilderInterface
    {
        if ($propertyValues === null || empty($propertyValues) || $propertyValues[0] === null) {
            return $this;
        }

        $queryString = null;
        $lastElemtentKey = count($propertyValues) - 1;
        foreach ($propertyValues as $key => $propertyValue) {
            $parameterName = ':' . md5($propertyName . '#' . count($this->where) . $key);
            $this->parameterMap[$parameterName] = $propertyValue;

            if ($key === 0) {
                $queryString .= '(';
            }
            if ($key !== $lastElemtentKey) {
                $queryString .= sprintf('("%s") = %s OR ', $propertyName, $parameterName);
            } else {
                $queryString .= sprintf('("%s") = %s )', $propertyName, $parameterName);
            }
        }

        $this->where[] = $queryString;

        return $this;
    }

    /**
     * Match any value which is like in the given array for the property
     *
     * @param string $propertyName
     * @param array $propertyValues
     * @return QueryBuilderInterface
     */
    public function likeAnyMatch(string $propertyName, array $propertyValues): QueryBuilderInterface
    {
        if ($propertyValues === null || empty($propertyValues) || $propertyValues[0] === null) {
            return $this;
        }

        $queryString = null;
        $lastElementKey = count($propertyValues) - 1;
        foreach ($propertyValues as $key => $propertyValue) {
            $parameterName = ':' . md5($propertyName . '#' . count($this->where) . $key);
            $this->parameterMap[$parameterName] = '%' . $propertyValue . '%';

            if ($key === 0) {
                $queryString .= '(';
            }
            if ($key !== $lastElementKey) {
                $queryString .= sprintf('("%s") LIKE %s OR ', $propertyName, $parameterName);
            } else {
                $queryString .= sprintf('("%s") LIKE %s)', $propertyName, $parameterName);
            }
        }

        $this->where[] = $queryString;

        return $this;
    }

    /**
     * @return string
     */
    protected function buildQueryString(): string
    {
        $whereString = implode(' AND ', $this->where);
        $orderString = implode(', ', $this->sorting);

        $queryString = 'SELECT * FROM "fulltext_objects" WHERE ' . $whereString;
        if (count($this->sorting)) {
            $queryString .= ' ORDER BY ' . $orderString;
        }

        if ($this->limit !== null) {
            $queryString .= ' LIMIT ' . $this->limit;
        }

        if ($this->from !== null) {
            $queryString .= ' OFFSET ' . $this->from;
        }

        return $queryString;
    }

    /**
     * @param string $propertyName
     * @param mixed $propertyValue
     * @param string $comparator Comparator sign i.e. '>' or '<='
     * @return QueryBuilderInterface
     */
    protected function compare($propertyName, $propertyValue, $comparator): QueryBuilderInterface
    {
        if ($propertyValue instanceof \DateTime) {
            $this->where[] = sprintf("datetime(`%s`) %s strftime('%s', '%s')", $propertyName, $comparator, '%Y-%m-%d %H:%M:%S', $propertyValue->format('Y-m-d H:i:s'));
        } else {
            $parameterName = ':' . md5($propertyName . '#' . count($this->where));
            $this->parameterMap[$parameterName] = $propertyValue;
            $this->where[] = sprintf('("%s") %s %s', $propertyName, $comparator, $parameterName);
        }

        return $this;
    }
}
