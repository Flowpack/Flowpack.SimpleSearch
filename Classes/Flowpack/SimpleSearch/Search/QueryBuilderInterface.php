<?php
namespace Flowpack\SimpleSearch\Search;

/**
 * Query Builder Interface for searches, currently unused due to problems with PHP 5.3.x
 */
interface QueryBuilderInterface {

	/**
	 * Sort descending by $propertyName
	 *
	 * @param string $propertyName the property name to sort by
	 * @return QueryBuilderInterface
	 */
	public function sortDesc($propertyName);


	/**
	 * Sort ascending by $propertyName
	 *
	 * @param string $propertyName the property name to sort by
	 * @return QueryBuilderInterface
	 */
	public function sortAsc($propertyName);


	/**
	 * output only $limit records
	 *
	 * @param integer $limit
	 * @return QueryBuilderInterface
	 */
	public function limit($limit);

	/**
	 * Start returned results $from number results.
	 *
	 * @param integer $from
	 * @return QueryBuilderInterface
	 */
	public function from($from);

	/**
	 * add an exact-match query for a given property
	 *
	 * @param string $propertyName
	 * @param mixed $propertyValue
	 * @return QueryBuilderInterface
	 */
	public function exactMatch($propertyName, $propertyValue);

	/**
	 * add an like query for a given property
	 *
	 * @param $propertyName
	 * @param $propertyValue
	 * @return QueryBuilderInterface
	 */
	public function like($propertyName, $propertyValue);

	/**
	 * Match the searchword against the fulltext index
	 *
	 * @param string $searchword
	 * @return QueryBuilderInterface
	 */
	public function fulltext($searchword);

	/**
	 * Execute the query and return the list of nodes as result
	 *
	 * @return array<\TYPO3\TYPO3CR\Domain\Model\NodeInterface>
	 */
	public function execute();

	/**
	 * Return the total number of hits for the query.
	 *
	 * @return integer
	 */
	public function count();

}
