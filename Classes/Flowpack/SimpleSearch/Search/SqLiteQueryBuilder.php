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
	public function limit($limit) {
		if ($limit) {
			$this->limit = $limit;
		}

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
		$this->where[] = "(" . $propertyName . " = '" . $propertyValue . "')";

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
		$this->where[] = "(" . $propertyName . " LIKE '%" . $propertyValue . "%')";

		return $this;
	}

	/**
	 * @param string $searchword
	 * @return QueryBuilderInterface
	 */
	public function fulltext($searchword) {
		$this->where[] = "(__identifier__ IN (SELECT __identifier__ FROM fulltext WHERE fulltext MATCH '" . $searchword . "' ORDER BY offsets(fulltext) ASC))";

		return $this;
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
		$possibleIdentifiers = array();
		foreach ($results as $result) {
			$possibleIdentifiers[] = $result['__identifier__'];
		}
		// PROTECTION
		$matchQuery = "SELECT snippet(fulltext, '$beginModifier', '$endModifier', '$ellipsis', -1, ($resultTokens * -1)) as snippet FROM fulltext WHERE fulltext MATCH '" . $searchword . "' AND __identifier__ IN ('" . implode("','", $possibleIdentifiers) . "') LIMIT 1;";
		$matchSnippet = $this->indexClient->query($matchQuery);
		if (isset($matchSnippet[0]['snippet']) && $matchSnippet[0]['snippet'] !== '') {
			$match = $matchSnippet[0]['snippet'];
		} else {
			$match = '';
		}
		return $match;
	}

	/**
	 * Execute the query and return the list of results
	 *
	 * @return array
	 */
	public function execute() {
		$query = $this->buildQueryString();
		$result = $this->indexClient->query($query);

		if (empty($result)) {
			return array();
		}

		return array_values($result);
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

	/**
	 * Return the total number of hits for the query.
	 *
	 * @return integer
	 */
	public function count() {
		$result = $this->execute();
		return count($result);
	}
}