<?php
namespace Flowpack\SimpleSearch\Search;

/**
 * Query Builder for searches
 */
class SqLiteQueryBuilder implements QueryBuilderInterface {

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
		$this->sorting[] = $propertyName . ' DESC';

		return $this;
	}


	/**
	 * Sort ascending by $propertyName
	 *
	 * @param string $propertyName the property name to sort by
	 * @return QueryBuilderInterface
	 */
	public function sortAsc($propertyName) {
		$this->sorting[] = $propertyName . ' ASC';

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
	 * @param string $searchword
	 * @return QueryBuilderInterface
	 */
	public function fulltext($searchword) {
		$this->where[] = "(rowid IN (SELECT rowid FROM fulltext WHERE fulltext MATCH '" . $searchword . "'))";

		return $this;
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

		$queryString = 'SELECT * FROM objects WHERE ' . $whereString;
		if (count($this->sorting)) {
			$queryString .= ' ORDER BY ' . $orderString;
		}

		if ($this->limit !== NULL) {
			$queryString .= ' LIMIT ' . $this->limit;
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