<?php
declare(strict_types=1);

namespace Flowpack\SimpleSearch\Factory;

use Flowpack\SimpleSearch\Domain\Service\IndexInterface;
use Flowpack\SimpleSearch\Exception;
use Neos\Flow\ObjectManagement\ObjectManagerInterface;

/**
 * This factory keeps track of existing index instances
 *
 * @api
 */
class IndexFactory
{
    /**
     * @var ObjectManagerInterface
     */
    protected $objectManager;

    /**
     * @var array
     */
    protected $searchIndexInstances = [];

    /**
     * @param ObjectManagerInterface $objectManager
     */
    public function injectObjectManager(ObjectManagerInterface $objectManager): void
    {
        $this->objectManager = $objectManager;
    }

    /**
     * @param string $indexName
     * @param string|null $indexType Class name for index instance
     * @return IndexInterface
     * @throws Exception
     */
    public function create(string $indexName, string $indexType = null): IndexInterface
    {
        if (!isset($indexType)) {
            if ($this->objectManager === null) {
                throw new Exception('If this package is used outside of a Neos Flow context you must specify the $indexType argument.', 1398018955);
            }

            $indexType = $this->objectManager->getClassNameByObjectName(IndexInterface::class);
        }

        $instanceIdentifier = md5($indexName . '#' . $indexType);

        if (!isset($this->searchIndexInstances[$instanceIdentifier])) {
            $this->searchIndexInstances[$instanceIdentifier] = new $indexType($indexName);
        }

        return $this->searchIndexInstances[$instanceIdentifier];
    }
}
