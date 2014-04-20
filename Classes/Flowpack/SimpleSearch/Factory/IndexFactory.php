<?php
namespace Flowpack\SimpleSearch\Factory;

/**
 * This factory keeps track of existing index instances
 *
 * @api
 */
class IndexFactory {

	/**
	 * @var \TYPO3\Flow\Object\ObjectManagerInterface
	 */
	protected $objectManager;

	/**
	 * @var array
	 */
	protected $searchIndexInstances = array();

	/**
	 * @param \TYPO3\Flow\Object\ObjectManagerInterface $objectManager
	 */
	public function injectObjectManager(\TYPO3\Flow\Object\ObjectManagerInterface $objectManager) {
		$this->objectManager = $objectManager;
	}

	/**
	 * @param string $indexName
	 * @param string $indexType Class name for index instance
	 * @return \Flowpack\SimpleSearch\Domain\Service\IndexInterface
	 * @throws \Flowpack\SimpleSearch\Exception
	 */
	public function create($indexName, $indexType = NULL) {
		if (!isset($indexType)) {
			if (!isset($this->objectManager)) {
				throw new \Flowpack\SimpleSearch\Exception('If this package is used outside of a TYPO3 Flow context you must specify the $indexType argument.', 1398018955);
			}

			$indexType = $this->objectManager->getClassNameByObjectName('Flowpack\SimpleSearch\Domain\Service\IndexInterface');
		}

		$instanceIdentifier = md5($indexName . '#' . $indexType);

		if (!isset($this->searchIndexInstances[$instanceIdentifier])) {
			$this->searchIndexInstances[$instanceIdentifier] = new $indexType($indexName);
		}

		return $this->searchIndexInstances[$instanceIdentifier];
	}
}