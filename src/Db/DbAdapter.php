<?php
declare(strict_types=1);

namespace GraphCards\Db;

use GraphCards\Model\Node;
use GraphCards\Model\Property;
use GraphCards\Model\PropertyValue;
use GraphCards\Model\Relationship;
use GraphCards\Utils\DbUtils;
use GraphAware\Neo4j\Client\Exception\Neo4jExceptionInterface;
use GraphAware\Neo4j\Client\Result\ResultCollection;


class DbAdapter
{
    const LIMIT_DEFAULT = 20;

    /** @var Db */
    protected $db;

    /** @var \Collator */
    protected $collator;


    /**
     * DbAdapter constructor.
     * @param Db $db
     */
    public function __construct(Db $db)
    {
        $this->db = $db;
    }


    /**
     * @param Node $node
     * @return Node
     */
    public function createNode(Node $node): Node
    {
        $propertyData = [];

        foreach ($node->getProperties() as $property) {
            $values = [];

            foreach ($property->getValues() as $propertyValue) {
                $values[] = $propertyValue->getValue();
            }

            $propertyData[$property->getName()] = $values;
        }

        $dbQuery = new DbQuery();
        $propertyQuery = DbUtils::propertiesString($propertyData, $dbQuery->bind);

        $dbQuery->query = sprintf
        (
            'CREATE (n%s { %s }) RETURN ID(n)',
            DbUtils::labelsString($node->getLabels()),
            $propertyQuery
        );

        $transaction = $this->db->beginTransaction();
        $transaction->push($dbQuery->query, $dbQuery->bind);
        $this->db->logQuery($dbQuery);

        try {
            $resultCollection = $this->db->commit($transaction);
            $nodeId = -1;

            /** @var ResultCollection $resultCollection */
            if ($resultCollection instanceof ResultCollection) {
                foreach ($resultCollection as $result) {
                    $nodeId = intval($result->firstRecord()->get('ID(n)'));
                    break;
                }
            }
        } catch (Neo4jExceptionInterface $exception) {
            $this->db->logException($exception);
            throw new \RuntimeException
            (
                sprintf
                (
                    '%s: Neo4j commit failed.',
                    __METHOD__
                ),
                0,
                $exception
            );
        }

        return $this->loadNodeById($nodeId);
    }


    /**
     * @param Node $newNode
     * @return Node
     */
    public function updateNode(Node $newNode): Node
    {
        $oldNode = $this->loadNode($newNode->getUuid());

        $oldProperties = [];

        foreach ($oldNode->getProperties() as $property) {
            if ($property->getName() === 'uuid') {
                continue;
            }

            $oldProperties[$property->getName()] = [];

            foreach ($property->getValues() as $propertyValue) {
                $oldProperties[$property->getName()][] = $propertyValue->getValue();
            }
        }

        $newProperties = [];

        foreach ($newNode->getProperties() as $property) {
            if ($property->getName() === 'uuid') {
                continue;
            }

            if (count($property->getValues()) === 0) {
                continue;
            }

            $newProperties[$property->getName()] = [];

            foreach ($property->getValues() as $propertyValue) {
                $newProperties[$property->getName()][] = $propertyValue->getValue();
            }
        }

        $updatedProperties = [];

        foreach ($newProperties as $key => $value) {
            if (isset($oldProperties[$key]) && (serialize($oldProperties[$key]) === serialize($newProperties[$key]))) {
                continue;
            }

            $updatedProperties[$key] = $newProperties[$key];
        }

        foreach (array_keys($oldProperties) as $key) {
            if (!isset($newProperties[$key])) {
                $updatedProperties[$key] = '';
            }
        }

        $oldLabels = $oldNode->getLabels();
        $newLabels = array_filter($newNode->getLabels());
        $addedLabels = array_diff($newLabels, $oldLabels);
        $removedLabels = array_diff($oldLabels, $newLabels);

        // Nothing changed at all?
        if ((count($updatedProperties) === 0) && (count($addedLabels) === 0) && (count($removedLabels) === 0)) {
            return $this->loadNode($newNode->getUuid());
        }

        $transaction = $this->db->beginTransaction();

        $dbQuery = new DbQuery();

        $dbQuery->bind = ['uuid' => $newNode->getUuid()];
        $propertyQuery = DbUtils::propertiesUpdateString('node', $updatedProperties, $dbQuery->bind);

        $dbQuery->query = sprintf
        (
            'MATCH (node { uuid: {uuid} })%s',
            $propertyQuery
        );

        if (count($removedLabels) > 0) {
            $dbQuery->query .= sprintf
            (
                ' REMOVE node%s',
                DbUtils::labelsString($removedLabels)
            );
        }

        if (count($addedLabels) > 0) {
            $dbQuery->query .= sprintf
            (
                ' SET node%s',
                DbUtils::labelsString($addedLabels)
            );
        }

        $transaction->push($dbQuery->query, $dbQuery->bind);
        $this->db->logQuery($dbQuery);

        try {
            $this->db->commit($transaction);
        } catch (Neo4jExceptionInterface $exception) {
            $this->db->logException($exception);
            throw new \RuntimeException
            (
                sprintf
                (
                    '%s: Neo4j commit failed.',
                    __METHOD__
                ),
                0,
                $exception
            );
        }

        return $this->loadNode($newNode->getUuid());
    }


    /**
     * @param int $nodeId
     * @return Node|null
     */
    protected function loadNodeById(int $nodeId): ?Node
    {
        $dbQuery = (new DbQuery())
            ->setQuery('MATCH (node) WHERE ID(node) = $id RETURN node')
            ->setBind(['id' => $nodeId]);

        $this->db->logQuery($dbQuery);

        try {
            $dbResult = new DbResult($this, $dbQuery, $this->db->runQuery($dbQuery));
        } catch (Neo4jExceptionInterface $exception) {
            $this->db->logException($exception);
            throw new \RuntimeException
            (
                sprintf
                (
                    '%s: Neo4j run failed.',
                    __METHOD__
                ),
                0,
                $exception
            );
        }

        return $dbResult->getFirstRecord()->getNode('node');
    }


    /**
     * @param int $nodeId
     * @return string
     */
    public function getNodeUuidById(int $nodeId): string
    {
        $dbQuery = (new DbQuery())
            ->setQuery('MATCH (node) WHERE ID(node) = $id RETURN node.uuid')
            ->setBind(['id' => $nodeId]);

        $this->db->logQuery($dbQuery);

        try {
            $qResult = $this->db->runQuery($dbQuery);
        } catch (Neo4jExceptionInterface $exception) {
            $this->db->logException($exception);
            throw new \RuntimeException
            (
                sprintf
                (
                    '%s: Neo4j run failed.',
                    __METHOD__
                ),
                0,
                $exception
            );
        }

        $node = new Node();

        foreach ($qResult->records() as $record) {
            return $record->get('node.uuid');
        }

        return '';
    }


    /**
     * @param string $nodeUuid
     * @return Node|null
     */
    public function loadNode(string $nodeUuid): ?Node
    {
        $dbQuery = (new DbQuery())
            ->setQuery('MATCH (node { uuid: {uuid} }) RETURN node')
            ->setBind(['uuid' => $nodeUuid]);

        $this->db->logQuery($dbQuery);

        try {
            $dbResult = new DbResult($this, $dbQuery, $this->db->runQuery($dbQuery));
        } catch (Neo4jExceptionInterface $exception) {
            $this->db->logException($exception);
            throw new \RuntimeException
            (
                sprintf
                (
                    '%s: Neo4j run failed.',
                    __METHOD__
                ),
                0,
                $exception
            );
        }

        if ($dbResult->isEmpty()) {
            return null;
        }

        return $dbResult->getFirstRecord()->getNode('node');
    }


    /**
     * @param \GraphAware\Neo4j\Client\Formatter\Type\Node $recordNode
     * @return Node
     */
    protected function loadNodeFromRecord(\GraphAware\Neo4j\Client\Formatter\Type\Node $recordNode): Node
    {
        $node = new Node();

        $node->setLabels($recordNode->labels());

        foreach ($recordNode->values() as $name => $value) {
            $node->setProperty(DbUtils::propertyFromValue($name, $value));
        }

        return $node;
    }


    /**
     * @param string $nodeUuid
     * @return void
     */
    public function deleteNode(string $nodeUuid): void
    {
        $dbQuery = (new DbQuery())
            ->setQuery('MATCH (node { uuid: {uuid} }) DELETE node')
            ->setBind(['uuid' => $nodeUuid]);

        $this->db->logQuery($dbQuery);

        try {
            $this->db->runQuery($dbQuery);
        } catch (Neo4jExceptionInterface $exception) {
            $this->db->logException($exception);
            throw new \RuntimeException
            (
                sprintf
                (
                    '%s: Neo4j run failed.',
                    __METHOD__
                ),
                0,
                $exception
            );
        }
    }


    /**
     * @param DbQuery $dbQuery
     * @return Node[]
     */
    public function listNodes(DbQuery $dbQuery): array
    {
        $nodes = [];
        $this->db->logQuery($dbQuery);

        try {
            $dbResult = new DbResult($this, $dbQuery, $this->db->runQuery($dbQuery));
        } catch (Neo4jExceptionInterface $exception) {
            $this->db->logException($exception);
            throw new \RuntimeException
            (
                sprintf
                (
                    '%s: Neo4j run failed.',
                    __METHOD__
                ),
                0,
                $exception
            );
        }

        foreach ($dbResult->getRecords() as $record) {
            $node = $record->getNode('node');

            if ($node === null) {
                continue;
            }

            if (strlen($node->getUuid()) === 0) {
                continue;
            }

            $nodes[] = $node;
        }

        return $nodes;
    }


    /**
     * @param string $label
     * @param int $skip
     * @param int Slimit
     * @return DbQuery
     */
    public function buildNodeQuery(string $label = '', int $skip = 0, int $limit = self::LIMIT_DEFAULT): DbQuery
    {
        return (new DbQuery())->setQuery(sprintf(
            'MATCH (node%s) RETURN node ORDER BY node.uuid SKIP %d LIMIT %d',
            DbUtils::labelsString([$label]),
            $skip,
            $limit
        ));
    }


    /**
     * @param string $relationshipUuid
     * @return Relationship|null
     */
    public function loadRelationship(string $relationshipUuid): ?Relationship
    {
        $dbQuery = (new DbQuery())
            ->setQuery('MATCH ()-[relationship { uuid: {uuid} }]->() RETURN relationship')
            ->setBind(['uuid' => $relationshipUuid]);

        $this->db->logQuery($dbQuery);

        try {
            $dbResult = new DbResult($this, $dbQuery, $this->db->runQuery($dbQuery));
        } catch (Neo4jExceptionInterface $exception) {
            $this->db->logException($exception);
            throw new \RuntimeException
            (
                sprintf
                (
                    '%s: Neo4j run failed.',
                    __METHOD__
                ),
                0,
                $exception
            );
        }

        return $dbResult->getFirstRecord()->getRelationship('relationship');
    }


    /**
     * @param int $relationshipId
     * @return Relationship|null
     */
    protected function loadRelationshipById(int $relationshipId): ?Relationship
    {
        $dbQuery = (new DbQuery())
            ->setQuery('MATCH ()-[rel]->() WHERE ID(rel) = $id RETURN rel')
            ->setBind(['id' => $relationshipId]);

        $this->db->logQuery($dbQuery);

        try {
            $dbResult = new DbResult($this, $dbQuery, $this->db->runQuery($dbQuery));
        } catch (Neo4jExceptionInterface $exception) {
            $this->db->logException($exception);
            throw new \RuntimeException
            (
                sprintf
                (
                    '%s: Neo4j run failed.',
                    __METHOD__
                ),
                0,
                $exception
            );
        }

        return $dbResult->getFirstRecord()->getRelationship('rel');
    }


    /**
     * @param DbQuery $dbQuery
     * @return Relationship[]
     */
    public function listRelationships(DbQuery $dbQuery): array
    {
        $relationships = [];

        $this->db->logQuery($dbQuery);

        try {
            $dbResult = new DbResult($this, $dbQuery, $this->db->runQuery($dbQuery));
        } catch (Neo4jExceptionInterface $exception) {
            $this->db->logException($exception);
            throw new \RuntimeException
            (
                sprintf
                (
                    '%s: Neo4j run failed.',
                    __METHOD__
                ),
                0,
                $exception
            );
        }

        foreach ($dbResult->getRecords() as $record) {
            $relationship = $record->getRelationship('r');

            if ($relationship === null) {
                continue;

            }
            if (strlen($relationship->getUuid()) === 0) {
                continue;
            }

            $relationships[] = $relationship;
        }

        return $relationships;
    }


    /**
     * @param int $limit
     * @return DbQuery
     */
    public function buildRelationshipQuery(int $limit): DbQuery
    {
        return (new DbQuery())
            ->setQuery('MATCH (n1)-[r]->(n2) RETURN r LIMIT ' . $limit);
    }


    /**
     * @param string $nodeUuid
     * @param bool $loadNodes
     * @return Relationship[]
     */
    public function listNodeRelationships(string $nodeUuid, bool $loadNodes = false): array
    {
        $relationships = [];

        $dbQuery = (new DbQuery())
            ->setQuery('MATCH (n1 {uuid: {n1uuid}})-[r]-(n2) RETURN r LIMIT 20')
            ->setBind(['n1uuid' => $nodeUuid]);

        $this->db->logQuery($dbQuery);

        try {
            $dbResult = new DbResult($this, $dbQuery, $this->db->runQuery($dbQuery));
        } catch (Neo4jExceptionInterface $exception) {
            $this->db->logException($exception);
            throw new \RuntimeException
            (
                sprintf
                (
                    '%s: Neo4j run failed.',
                    __METHOD__
                ),
                0,
                $exception
            );
        }

        foreach ($dbResult->getRecords() as $record) {
            $relationship = $record->getRelationship('r');

            if ($relationship === null) {
                continue;
            }

            if (strlen($relationship->getUuid()) === 0) {
                continue;
            }

            if ($loadNodes) {
                $relationship->setSourceNode($this->loadNode($relationship->getSourceNode()->getUuid()));
                $relationship->setTargetNode($this->loadNode($relationship->getTargetNode()->getUuid()));
            }

            $relationships[] = $relationship;
        }

        return $relationships;
    }


    /**
     * @param Property[] $properties
     * @param array $bind
     * @return string
     */
    protected function propertiesString(array $properties, &$bind): string
    {
        $propertyData = [];

        foreach ($properties as $property) {
            $values = [];

            foreach ($property->getValues() as $propertyValue) {
                $values[] = $propertyValue->getValue();
            }

            $propertyData[$property->getName()] = $values;
        }

        return DbUtils::propertiesString($propertyData, $bind);
    }


    /**
     * @param Relationship $relationship
     * @return Relationship
     */
    public function createRelationship(Relationship $relationship): Relationship
    {
        $dbQuery = new DbQuery();

        $dbQuery->query = sprintf
        (
            'MATCH (s%s {%s}), (t%s {%s}) MERGE (s)-[r%s {%s}]->(t) RETURN ID(r)',
            DbUtils::labelsString($relationship->getSourceNode()->getLabels()),
            $this->propertiesString($relationship->getSourceNode()->getProperties(), $dbQuery->bind),
            DbUtils::labelsString($relationship->getTargetNode()->getLabels()),
            $this->propertiesString($relationship->getTargetNode()->getProperties(), $dbQuery->bind),
            DbUtils::labelsString([$relationship->getType()]),
            $this->propertiesString($relationship->getProperties(), $dbQuery->bind)
        );

        $transaction = $this->db->beginTransaction();
        $transaction->push($dbQuery->query, $dbQuery->bind);
        $this->db->logQuery($dbQuery);

        try {
            $resultCollection = $this->db->commit($transaction);
            $relationshipId = -1;

            /** @var ResultCollection $resultCollection */
            if ($resultCollection instanceof ResultCollection) {
                foreach ($resultCollection as $result) {
                    $relationshipId = intval($result->firstRecord()->get('ID(r)'));
                    break;
                }
            }
        } catch (Neo4jExceptionInterface $exception) {
            $this->db->logException($exception);
            throw new \RuntimeException
            (
                sprintf
                (
                    '%s: Neo4j commit failed.',
                    __METHOD__
                ),
                0,
                $exception
            );
        }

        return $this->loadRelationshipById($relationshipId);
    }


    /**
     * @param Relationship $newRelationship
     * @return Relationship
     */
    public function updateRelationship(Relationship $newRelationship): Relationship
    {
        $oldRelationship = $this->loadRelationship($newRelationship->getUuid());

        $oldProperties = [];

        foreach ($oldRelationship->getProperties() as $property) {
            if ($property->getName() === 'uuid') {
                continue;
            }

            $oldProperties[$property->getName()] = [];

            foreach ($property->getValues() as $propertyValue) {
                $oldProperties[$property->getName()][] = $propertyValue->getValue();
            }
        }

        $newProperties = [];

        foreach ($newRelationship->getProperties() as $property) {
            if ($property->getName() === 'uuid') {
                continue;
            }

            if (count($property->getValues()) === 0) {
                continue;
            }

            $newProperties[$property->getName()] = [];

            foreach ($property->getValues() as $propertyValue) {
                $newProperties[$property->getName()][] = $propertyValue->getValue();
            }
        }

        $updatedProperties = [];

        foreach ($newProperties as $key => $value) {
            if (isset($oldProperties[$key]) && (serialize($oldProperties[$key]) === serialize($newProperties[$key]))) {
                continue;
            }

            $updatedProperties[$key] = $newProperties[$key];
        }

        foreach (array_keys($oldProperties) as $key) {
            if (!isset($newProperties[$key])) {
                $updatedProperties[$key] = '';
            }
        }

        if ($newRelationship->getType() !== $oldRelationship->getType()) {
            throw new \RuntimeException('You cannot change the relationship type.');
        }

        if ($newRelationship->getSourceNode()->getUuid() !== $oldRelationship->getSourceNode()->getUuid()) {
            throw new \RuntimeException('You cannot change the relationship source node.');
        }

        if ($newRelationship->getTargetNode()->getUuid() !== $oldRelationship->getTargetNode()->getUuid()) {
            throw new \RuntimeException('You cannot change the relationship target node.');
        }

        // Nothing changed at all?
        if (count($updatedProperties) === 0) {
            return $this->loadRelationship($newRelationship->getUuid());
        }

        $transaction = $this->db->beginTransaction();

        $dbQuery = (new DbQuery())
            ->setBind(['uuid' => $newRelationship->getUuid()]);

        $propertyQuery = DbUtils::propertiesUpdateString('relationship', $updatedProperties, $dbQuery->bind);

        $dbQuery->query = sprintf(
            'MATCH ()-[relationship { uuid: {uuid} }]->()%s',
            $propertyQuery
        );

        $transaction->push($dbQuery->query, $dbQuery->bind);
        $this->db->logQuery($dbQuery);

        try {
            $this->db->commit($transaction);
        } catch (Neo4jExceptionInterface $exception) {
            $this->db->logException($exception);
            throw new \RuntimeException
            (
                sprintf
                (
                    '%s: Neo4j commit failed.',
                    __METHOD__
                ),
                0,
                $exception
            );
        }

        return $this->loadRelationship($newRelationship->getUuid());
    }


    /**
     * @return string[]
     */
    public function listNodeLabels(): array
    {
        $result = [];

        $dbQuery = (new DbQuery())->setQuery('CALL db.labels()');
        $this->db->logQuery($dbQuery);

        try {
            $qResult = $this->db->runQuery($dbQuery);
        } catch (Neo4jExceptionInterface $exception) {
            $this->db->logException($exception);
            throw new \RuntimeException
            (
                sprintf(
                    '%s: Neo4j run failed.',
                    __METHOD__
                ),
                0,
                $exception
            );

            $this->sort($result);
        }

        foreach ($qResult->records() as $record) {
            $result[] = $record->get('label');
        }

        $this->sort($result);

        return $result;
    }


    /**
     * @return string[]
     */
    public function listRelationshipTypes(): array
    {
        $result = [];

        $dbQuery = (new DbQuery())->setQuery('CALL db.relationshipTypes()');
        $this->db->logQuery($dbQuery);

        try {
            $qResult = $this->db->runQuery($dbQuery);
        } catch (Neo4jExceptionInterface $exception) {
            $this->db->logException($exception);
            throw new \RuntimeException
            (
                sprintf
                (
                    '%s: Neo4j run failed.',
                    __METHOD__
                ),
                0,
                $exception
            );
        }

        foreach ($qResult->records() as $record) {
            $result[] = $record->get('relationshipType');
        }

        $this->sort($result);

        return $result;
    }


    /**
     * @return string[]
     */
    public function listPropertyKeys(): array
    {
        $result = [];

        $dbQuery = (new DbQuery())->setQuery('CALL db.propertyKeys()');
        $this->db->logQuery($dbQuery);

        try {
            $qResult = $this->db->runQuery($dbQuery);
        } catch (Neo4jExceptionInterface $exception) {
            $this->db->logException($exception);
            throw new \RuntimeException
            (
                sprintf
                (
                    '%s: Neo4j run failed.',
                    __METHOD__
                ),
                0,
                $exception
            );
        }

        foreach ($qResult->records() as $record) {
            $result[] = $record->get('propertyKey');
        }

        $this->sort($result);

        return $result;
    }


    /**
     * @param DbQuery $dbQuery
     * @return DbResult
     */
    public function getResult(DbQuery $dbQuery): DbResult
    {
        $rows = [];
        $this->db->logQuery($dbQuery);

        try {
            $dbResult = new DbResult($this, $dbQuery, $this->db->runQuery($dbQuery));
        } catch (Neo4jExceptionInterface $exception) {
            $this->db->logException($exception);
            throw new \RuntimeException(
                sprintf(
                    '%s: Neo4j run failed.',
                    __METHOD__
                ),
                0,
                $exception
            );
        }

        return $dbResult;
    }


    /**
     * @param string[] $arr
     * @return void
     */
    protected function sort(array &$arr): void
    {
        if (!$this->collator) {
            // TODO: Fix hardcoded locale
            $this->collator = new \Collator('en');
        }

        usort($arr, [$this->collator, 'compare']);
    }
}