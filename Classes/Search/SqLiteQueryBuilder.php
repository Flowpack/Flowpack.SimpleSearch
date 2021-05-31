<?php
declare(strict_types=1);

namespace Flowpack\SimpleSearch\Search;

use Flowpack\SimpleSearch\Domain\Service\IndexInterface;
use Neos\Flow\Annotations as Flow;

/**
 * Query Builder for searches
 *
 * Note: some signatures are not as strict as in the interfaces, because two query builder interfaces are "mixed"
 * in classes implementing this one
 */
class SqLiteQueryBuilder implements QueryBuilderInterface
{
    /**
     * @Flow\Inject
     * @var IndexInterface
     */
    protected $indexClient;

    /**
     * @var int
     */
    protected $limit;

    /**
     * @var int
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
     * Sort descending by $propertyName
     *
     * @param string $propertyName the property name to sort by
     * @return QueryBuilderInterface
     */
    public function sortDesc(string $propertyName): QueryBuilderInterface
    {
        $this->sorting[] = 'objects.' . $propertyName . ' DESC';

        return $this;
    }

    /**
     * Sort ascending by $propertyName
     *
     * @param string $propertyName the property name to sort by
     * @return QueryBuilderInterface
     */
    public function sortAsc(string $propertyName): QueryBuilderInterface
    {
        $this->sorting[] = 'objects.' . $propertyName . ' ASC';

        return $this;
    }

    /**
     * output only $limit records
     *
     * @param int|null $limit
     * @return QueryBuilderInterface
     */
    public function limit($limit): QueryBuilderInterface
    {
        $this->limit = $limit === null ? $limit : (int)$limit;
        return $this;
    }

    /**
     * Start returned results $from number results.
     *
     * @param int|null $from
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
     * @param mixed $propertyValue
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
        $this->where[] = '(`' . $propertyName . '` LIKE ' . $parameterName . ')';
        $this->parameterMap[$parameterName] = '%' . $propertyValue . '%';

        return $this;
    }

    /**
     * Add a custom condition
     *
     * @param string $conditon
     * @return QueryBuilderInterface
     */
    public function customCondition(string $conditon): QueryBuilderInterface
    {
        $this->where[] = $conditon;

        return $this;
    }

    /**
     * @param string $searchword
     * @return QueryBuilderInterface
     */
    public function fulltext(string $searchword): QueryBuilderInterface
    {
        $parameterName = ':' . md5('FULLTEXT#' . count($this->where));
        $this->where[] = '(__identifier__ IN (SELECT __identifier__ FROM fulltext WHERE fulltext MATCH ' . $parameterName . ' ORDER BY offsets(fulltext) ASC))';
        $this->parameterMap[$parameterName] = $searchword;

        return $this;
    }

    /**
     * add a greater than query for a given property
     *
     * @param string $propertyName
     * @param mixed $propertyValue
     * @return QueryBuilderInterface
     */
    public function greaterThan(string $propertyName, $propertyValue): QueryBuilderInterface
    {
        return $this->compare($propertyName, $propertyValue, '>');
    }

    /**
     * add a greater than or equal query for a given property
     *
     * @param string $propertyName
     * @param mixed $propertyValue
     * @return QueryBuilderInterface
     */
    public function greaterThanOrEqual(string $propertyName, $propertyValue): QueryBuilderInterface
    {
        return $this->compare($propertyName, $propertyValue, '>=');
    }

    /**
     * add a less than query for a given property
     *
     * @param string $propertyName
     * @param mixed $propertyValue
     * @return QueryBuilderInterface
     */
    public function lessThan(string $propertyName, $propertyValue): QueryBuilderInterface
    {
        return $this->compare($propertyName, $propertyValue, '<');
    }

    /**
     * add a less than or equal query for a given property
     *
     * @param string $propertyName
     * @param mixed $propertyValue
     * @return QueryBuilderInterface
     */
    public function lessThanOrEqual(string $propertyName, $propertyValue): QueryBuilderInterface
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
     * @return int
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
     * @param int $resultTokens The amount of tokens (words) to get surrounding the match hit. (defaults to 60)
     * @param string $ellipsis added to the end of the string if the text was longer than the snippet produced. (defaults to "...")
     * @param string $beginModifier added immediately before the searchword in the snippet (defaults to <b>)
     * @param string $endModifier added immediately after the searchword in the snippet (defaults to </b>)
     * @return string
     */
    public function fulltextMatchResult(string $searchword, int $resultTokens = 60, string $ellipsis = '...', string $beginModifier = '<b>', string $endModifier = '</b>'): string
    {
        $query = $this->buildQueryString();
        $results = $this->indexClient->executeStatement($query, $this->parameterMap);
        // SQLite3 has a hard-coded limit of 999 query variables, so we split the $result in chunks
        // of 990 elements (we need some space for our own variables), query these, and return the first result.
        // @see https://sqlite.org/limits.html -> "Maximum Number Of Host Parameters In A Single SQL Statement"
        $chunks = array_chunk($results, 990);
        foreach ($chunks as $chunk) {
            $queryParameters = [];
            $identifierParameters = [];
            foreach ($chunk as $key => $result) {
                $parameterName = ':possibleIdentifier' . $key;
                $identifierParameters[] = $parameterName;
                $queryParameters[$parameterName] = $result['__identifier__'];
            }

            $queryParameters[':beginModifier'] = $beginModifier;
            $queryParameters[':endModifier'] = $endModifier;
            $queryParameters[':ellipsis'] = $ellipsis;
            $queryParameters[':resultTokens'] = ($resultTokens * -1);

            $matchQuery = 'SELECT snippet(fulltext, :beginModifier, :endModifier, :ellipsis, -1, :resultTokens) as snippet FROM fulltext WHERE fulltext MATCH :searchword AND __identifier__ IN (' . implode(',', $identifierParameters) . ') LIMIT 1;';
            $queryParameters[':searchword'] = $searchword;
            $matchSnippet = $this->indexClient->executeStatement($matchQuery, $queryParameters);

            // If we have a hit here, we stop searching and return it.
            if (isset($matchSnippet[0]['snippet']) && $matchSnippet[0]['snippet'] !== '') {
                return $matchSnippet[0]['snippet'];
            }
        }
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
                $queryString .= sprintf('(`%s`) = %s OR ', $propertyName, $parameterName);
            } else {
                $queryString .= sprintf('(`%s`) = %s )', $propertyName, $parameterName);
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
        $lastElemtentKey = count($propertyValues) - 1;
        foreach ($propertyValues as $key => $propertyValue) {
            $parameterName = ':' . md5($propertyName . '#' . count($this->where) . $key);
            $this->parameterMap[$parameterName] = '%' . $propertyValue . '%';

            if ($key === 0) {
                $queryString .= '(';
            }
            if ($key !== $lastElemtentKey) {
                $queryString .= sprintf('(`%s`) LIKE %s OR ', $propertyName, $parameterName);
            } else {
                $queryString .= sprintf('(`%s`) LIKE %s)', $propertyName, $parameterName);
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

        $queryString = 'SELECT DISTINCT(__identifier__), * FROM objects WHERE ' . $whereString;
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
    protected function compare(string $propertyName, $propertyValue, string $comparator): QueryBuilderInterface
    {
        if ($propertyValue instanceof \DateTime) {
            $this->where[] = sprintf("datetime(`%s`) %s strftime('%s', '%s')", $propertyName, $comparator, '%Y-%m-%d %H:%M:%S', $propertyValue->format('Y-m-d H:i:s'));
        } else {
            $parameterName = ':' . md5($propertyName . '#' . count($this->where));
            $this->parameterMap[$parameterName] = $propertyValue;
            $this->where[] = sprintf('(`%s`) %s %s', $propertyName, $comparator, $parameterName);
        }

        return $this;
    }
}
