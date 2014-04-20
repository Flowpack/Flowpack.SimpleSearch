<?php
namespace Flowpack\SimpleSearch\Domain\Service;


interface IndexInterface {

	/**
	 * @param $identifier identifier for the data
	 * @param array $properties Properties to put into index
	 * @param string $fullText string to push to fulltext index for this property
	 * @return void
	 */
	public function indexData($identifier, $properties, $fullText);

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
	 * @return void
	 */
	public function flush();
}