<?php
namespace Flowpack\SimpleSearch\Search;

/**
 * Query Builder for searches
 */
class SqLiteQueryBuilder {

	/**
	 * @var \Flowpack\SimpleSearch\Domain\Service\IndexInterface
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
	protected $sorting = array();

	/**
	 * where clauses
	 *
	 * @var array
	 */
	protected $where = array();

	/**
	 * Map of query parameters to bind to the final statement.
	 *
	 * @var array
	 */
	protected $parameterMap = array();

	/**
	 * Injection method used by Flow dependency injection
	 *
	 * @param \Flowpack\SimpleSearch\Domain\Service\IndexInterface $indexClient
	 */
	public function injectIndexClient(\Flowpack\SimpleSearch\Domain\Service\IndexInterface $indexClient) {
		$this->indexClient = $indexClient;
	}

	/**
	 * Sort descending by $propertyName
	 *
	 * @param string $propertyName the property name to sort by
	 * @return QueryBuilderInterface
	 */
	public function sortDesc($propertyName) {
		$this->sorting[] = 'objects.' . $propertyName . ' DESC';

		return $this;
	}


	/**
	 * Sort ascending by $propertyName
	 *
	 * @param string $propertyName the property name to sort by
	 * @return QueryBuilderInterface
	 */
	public function sortAsc($propertyName) {
		$this->sorting[] = 'objects.' . $propertyName . ' ASC';

		return $this;
	}

	/**
	 * output only $limit records
	 *
	 * @param integer $limit
	 * @return QueryBuilderInterface
	 */
	public function limit($limit = NULL) {
		if ($limit !== NULL) {
			$limit = intval($limit);
		}
		$this->limit = $limit;
		return $this;
	}

	/**
	 * Start returned results $from number results.
	 *
	 * @param integer $from
	 * @return QueryBuilderInterface
	 */
	public function from($from = NULL) {
		if ($from !== NULL) {
			$from = intval($from);
		}

		$this->from = intval($from);
		return $this;
	}

	/**
	 * add an exact-match query for a given property
	 *
	 * @param string $propertyName
	 * @param string $propertyValue
	 * @return QueryBuilderInterface
	 */
	public function exactMatch($propertyName, $propertyValue) {
		$parameterName = ':' . md5($propertyName . '#' . count($this->where));
		$this->parameterMap[$parameterName] = $propertyValue;
		$this->where[] = sprintf("(`%s`) = %s", $propertyName, $parameterName);

		return $this;
	}

	/**
	 * add an like query for a given property
	 *
	 * @param $propertyName
	 * @param $propertyValue
	 * @return QueryBuilderInterface
	 */
	public function like($propertyName, $propertyValue) {
		$parameterName = ':' . md5($propertyName . '#' . count($this->where));
		$this->where[] = '(`' . $propertyName . '` LIKE ' . $parameterName . ')';
		$this->parameterMap[$parameterName] = '%' . $propertyValue . '%';

		return $this;
	}

	/**
	 * @param string $searchword
	 * @return QueryBuilderInterface
	 */
	public function fulltext($searchword) {
		$parameterName = ':' . md5('FULLTEXT#' . count($this->where));
		$this->where[] = "(__identifier__ IN (SELECT __identifier__ FROM fulltext WHERE fulltext MATCH " . $parameterName . " ORDER BY offsets(fulltext) ASC))";
		$this->parameterMap[$parameterName] = $searchword;

		return $this;
	}

	/**
	 * Execute the query and return the list of results
	 *
	 * @return array
	 */
	public function execute() {
		$query = $this->buildQueryString();
		$result = $this->indexClient->executeStatement($query, $this->parameterMap);

		if (empty($result)) {
			return array();
		}

		return array_values($result);
	}

	/**
	 * Return the total number of hits for the query.
	 *
	 * @return integer
	 */
	public function count() {
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
	public function fulltextMatchResult($searchword, $resultTokens = 60, $ellipsis = '...', $beginModifier = '<b>', $endModifier = '</b>') {
		$query = $this->buildQueryString();
		$results = $this->indexClient->query($query);
		$queryParameters = array();
		$identifierParameters = array();
		foreach ($results as $key => $result) {
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

		if (isset($matchSnippet[0]['snippet']) && $matchSnippet[0]['snippet'] !== '') {
			$match = $matchSnippet[0]['snippet'];
		} else {
			$match = '';
		}
		return $match;
	}

	/**
	 * Match any value in the given array for the property
	 *
	 * @param string $propertyName
	 * @param array $propertyValues
	 * @return QueryBuilder
	 */
	public function anyMatch($propertyName, $propertyValues) {
		if ($propertyValues === null || empty($propertyValues) || $propertyValues[0] === null) {
			return $this;
		}

		$queryString = null;
		$lastElemtentKey = count($propertyValues) - 1;
		foreach($propertyValues as $key => $propertyValue) {
			$parameterName = ':' . md5($propertyName . '#' . count($this->where) . $key);
			$this->parameterMap[$parameterName] = $propertyValue;

			if ($key === 0) {
				$queryString .= '(';
			}
			if ($key !== $lastElemtentKey) {
				$queryString .= sprintf("(`%s`) = %s OR ", $propertyName, $parameterName);
			} else {
				$queryString .= sprintf("(`%s`) = %s )", $propertyName, $parameterName);
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
	 * @return QueryBuilder
	 */
	public function likeAnyMatch($propertyName, $propertyValues) {
		if ($propertyValues === null || empty($propertyValues) || $propertyValues[0] === null) {
			return $this;
		}

		$queryString = null;
		$lastElemtentKey = count($propertyValues) - 1;
		foreach($propertyValues as $key => $propertyValue) {
			$parameterName = ':' . md5($propertyName . '#' . count($this->where) . $key);
			$this->parameterMap[$parameterName] = '%'.$propertyValue.'%';

			if ($key === 0) {
				$queryString .= '(';
			}
			if ($key !== $lastElemtentKey) {
				$queryString .= sprintf("(`%s`) LIKE %s OR ", $propertyName, $parameterName);
			} else {
				$queryString .= sprintf("(`%s`) LIKE %s)", $propertyName, $parameterName);
			}
		}

		$this->where[] = $queryString;

		return $this;
	}

	/**
	 * Add a range filter (gt) for the given property
	 *
	 * @param string $propertyName Name of the property
	 * @param mixed $value Value for comparison
	 * @return QueryBuilder
	 * @api
	 */
	public function greaterThan($propertyName, $value) {
		$this->where[] = sprintf(" (`%s`) > %s", $propertyName, $value);

		return $this;
	}

	/**
	 * Add a range filter (lt) for the given property
	 *
	 * @param string $propertyName Name of the property
	 * @param mixed $value Value for comparison
	 * @return QueryBuilder
	 * @api
	 */
	public function lessThan($propertyName, $value) {
		$this->where[] = sprintf(" (`%s`) < %s", $propertyName, $value);

		return $this;
	}

	/**
	 * Add a range filter (gte) for the given property
	 *
	 * @param string $propertyName Name of the property
	 * @param mixed $value Value for comparison
	 * @return QueryBuilder
	 * @api
	 */
	public function greaterThanOrEqual($propertyName, $value) {
		$this->where[] = sprintf(" (`%s`) >= %s", $propertyName, $value);

		return $this;
	}

	/**
	 * Add a range filter (lte) for the given property
	 *
	 * @param string $propertyName Name of the property
	 * @param mixed $value Value for comparison
	 * @return QueryBuilder
	 * @api
	 */
	public function lessThanOrEqual($propertyName, $value) {
		$this->where[] = sprintf(" (`%s`) <= %s", $propertyName, $value);

		return $this;
	}

	/**
	 * @return string
	 */
	protected function buildQueryString() {
		$whereString = implode(' AND ', $this->where);
		$orderString = implode(', ', $this->sorting);

		$queryString = 'SELECT DISTINCT(__identifier__), * FROM objects WHERE ' . $whereString;
		if (count($this->sorting)) {
			$queryString .= ' ORDER BY ' . $orderString;
		}

		if ($this->limit !== NULL) {
			$queryString .= ' LIMIT ' . $this->limit;
		}

		if ($this->from !== NULL) {
			$queryString .= ' OFFSET ' . $this->from;
		}

		return $queryString;
	}
}
