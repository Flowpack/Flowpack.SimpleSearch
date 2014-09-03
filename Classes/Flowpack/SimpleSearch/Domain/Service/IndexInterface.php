<?php
namespace Flowpack\SimpleSearch\Domain\Service;


interface IndexInterface {

	/**
	 * @param string $identifier identifier for the data
	 * @param array $properties Properties to put into index
	 * @param string $fullText string to push to fulltext index for this property
	 * @return void
	 */
	public function indexData($identifier, $properties, $fullText);

	/**
	 * Use this only to update properties on existing entries. For new entries use "indexData"
	 * This method is subject to change and could be removed from the interface at any time.
	 *
	 * @param array $properties
	 * @param string $identifier
	 * @return void
	 */
	public function insertOrUpdatePropertiesToIndex($properties, $identifier);

	/**
	 * @param string $identifier
	 * @return void
	 */
	public function removeData($identifier);

	/**
	 * @param string $query
	 * @return array
	 */
	public function query($query);

	/**
	 * @return string
	 */
	public function getIndexName();

	/**
	 * @param array $fulltext
	 * @param string $identifier
	 * @return void
	 */
	public function addToFulltext($fulltext, $identifier);

	/**
	 * Empty the index.
	 *
	 * @return void
	 */
	public function flush();

	/**
	 * Optimize the index, can be a void operation if the index has no such operation.
	 *
	 * @return void
	 */
	public function optimize();
}