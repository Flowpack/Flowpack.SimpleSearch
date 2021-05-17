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
     * @param int|null $limit
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
     * @param int $resultTokens The amount of characters to get surrounding the match hit. (defaults to 200)
     * @param string $ellipsis added to the end of the string if the text was longer than the snippet produced. (defaults to "...")
     * @param string $beginModifier added immediately before the searchword in the snippet (defaults to <b>)
     * @param string $endModifier added immediately after the searchword in the snippet (defaults to </b>)
     * @return string
     * @see https://github.com/boyter/php-excerpt
     */
    public function fulltextMatchResult($searchword, $resultTokens = 200, $ellipsis = '...', $beginModifier = '<b>', $endModifier = '</b>'): string
    {
        $searchword = trim($searchword);

        $query = $this->buildQueryString();
        $results = $this->indexClient->executeStatement($query, []);

        if ($results === []) {
            return '';
        }

        $matches = [];
        foreach ($results[0] as $indexedFieldName => $indexedFieldContent) {
            if (!empty($indexedFieldContent) && strpos($indexedFieldName, '_') !== 0) {
                $matches[] = trim(strip_tags((string)$indexedFieldContent));
            }
        }
        $matchContent = implode(' ', $matches);

        $searchWordParts = explode(' ', $searchword);
        $matchContent = preg_replace(
            array_map(static function (string $searchWordPart) {
                return sprintf('/(%s)/iu', preg_quote($searchWordPart, '/'));
            }, $searchWordParts),
            array_fill(0, count($searchWordParts), sprintf('%s$1%s', $beginModifier, $endModifier)),
            $matchContent
        );

        $matchLength = strlen($matchContent);
        if ($matchLength <= $resultTokens) {
            return $matchContent;
        }

        $locations = $this->extractLocations($searchWordParts, $matchContent);
        $snippetLocation = $this->determineSnippetLocation($locations, (int)($resultTokens / 3));

        return $this->extractSnippet($resultTokens, $ellipsis, $matchLength, $snippetLocation, $matchContent);
    }

    /**
     * find the locations of each of the words
     * Nothing exciting here. The array_unique is required
     * unless you decide to make the words unique before passing in
     *
     * @param array $words
     * @param string $fulltext
     * @return array
     * @see https://github.com/boyter/php-excerpt
     */
    private function extractLocations(array $words, string $fulltext): array
    {
        $locations = [];
        foreach ($words as $word) {
            $loc = stripos($fulltext, $word);
            while ($loc !== false) {
                $locations[0] = $loc;
                $loc = stripos($fulltext, $word, $loc + strlen($word));
            }
        }

        $locations = array_unique($locations);

        sort($locations);
        return $locations;
    }

    /**
     * Work out which is the most relevant portion to display
     *
     * This is done by looping over each match and finding the smallest distance between two found
     * strings. The idea being that the closer the terms are the better match the snippet would be.
     * When checking for matches we only change the location if there is a better match.
     * The only exception is where we have only two matches in which case we just take the
     * first as will be equally distant.
     *
     * @param array $locations
     * @param int $relativePosition
     * @return int
     * @see https://github.com/boyter/php-excerpt
     */
    private function determineSnippetLocation(array $locations, int $relativePosition): int
    {
        $locationsCount = count($locations);
        $smallestDiff = PHP_INT_MAX;

        if ($locationsCount === 0) {
            return 0;
        }

        $startPosition = $locations[0];
        if ($locationsCount > 2) {
            // skip the first as we check 1 behind
            for ($i = 1; $i < $locationsCount; $i++) {
                if ($i === $locationsCount - 1) { // at the end
                    $diff = $locations[$i] - $locations[$i - 1];
                } else {
                    $diff = $locations[$i + 1] - $locations[$i];
                }

                if ($smallestDiff > $diff) {
                    $smallestDiff = $diff;
                    $startPosition = $locations[$i];
                }
            }
        }

        return $startPosition > $relativePosition ? $startPosition - $relativePosition : 0;
    }

    /**
     * @param int $resultTokens
     * @param string $ellipsis
     * @param int $matchLength
     * @param int $snippetLocation
     * @param string $matchContent
     * @return string
     */
    private function extractSnippet(int $resultTokens, string $ellipsis, int $matchLength, int $snippetLocation, string $matchContent): string
    {
        if ($matchLength - $snippetLocation < $resultTokens) {
            $snippetLocation = (int)($snippetLocation - ($matchLength - $snippetLocation) / 2);
        }

        $snippet = substr($matchContent, $snippetLocation, $resultTokens);

        if ($snippetLocation + $resultTokens < $matchLength) {
            $snippet = substr($snippet, 0, strrpos($snippet, ' ')) . $ellipsis;
        }

        if ($snippetLocation !== 0) {
            $snippet = $ellipsis . substr($snippet, strpos($snippet, ' ') + 1);
        }

        return $snippet;
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
