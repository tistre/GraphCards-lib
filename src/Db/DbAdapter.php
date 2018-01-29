<?php

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
     * @return Node
     */
    protected function loadNodeById($nodeId): Node
    {
        $dbQuery = (new DbQuery())
            ->setQuery('MATCH (node) WHERE ID(node) = $id RETURN node')
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
            $node = $this->loadNodeFromRecord($record->get('node'));
        }

        return $node;
    }


    /**
     * @param string $nodeId
     * @return string
     */
    public function getNodeUuidById($nodeId): string
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
     * @return Node
     */
    public function loadNode($nodeUuid): Node
    {
        $dbQuery = (new DbQuery())
            ->setQuery('MATCH (node { uuid: {uuid} }) RETURN node')
            ->setBind(['uuid' => $nodeUuid]);

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
            $node = $this->loadNodeFromRecord($record->get('node'));
        }

        return $node;
    }


    /**
     * @param $recordNode
     * @return Node
     */
    protected function loadNodeFromRecord(\GraphAware\Neo4j\Client\Formatter\Type\Node $recordNode): Node
    {
        $node = new Node();

        $node->setLabels($recordNode->labels());

        foreach ($recordNode->values() as $name => $value) {
            $node->setProperty($this->valueToProperty($name, $value));
        }

        return $node;
    }


    /**
     * @param string $name
     * @param int|float|bool|string|array $values
     * @return Property
     */
    protected function valueToProperty(string $name, $values): Property
    {
        $property = (new Property())
            ->setName($name);

        if (!is_array($values)) {
            $values = [$values];
        }

        foreach ($values as $value) {
            if (is_int($value)) {
                $type = PropertyValue::TYPE_INTEGER;
            } elseif (is_float($value)) {
                $type = PropertyValue::TYPE_FLOAT;
            } elseif (is_bool($value)) {
                $type = PropertyValue::TYPE_BOOLEAN;
            } else {
                $type = PropertyValue::TYPE_STRING;
            }

            $propertyValue = (new PropertyValue())
                ->setType($type)
                ->setValue($value);

            $property->addValue($propertyValue);
        }

        return $property;
    }


    /**
     * @param string $nodeUuid
     * @return void
     */
    public function deleteNode($nodeUuid)
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
            $node = $this->loadNodeFromRecord($record->get('node'));

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
     * @return Relationship
     */
    public function loadRelationship($relationshipUuid): Relationship
    {
        $dbQuery = (new DbQuery())
            ->setQuery('MATCH ()-[relationship { uuid: {uuid} }]->() RETURN relationship')
            ->setBind(['uuid' => $relationshipUuid]);

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

        $relationship = new Node();

        foreach ($qResult->records() as $record) {
            $relationship = $this->loadRelationshipFromRecord($record->get('relationship'));
        }

        return $relationship;
    }


    /**
     * @param int $relationshipId
     * @return Relationship
     */
    protected function loadRelationshipById($relationshipId): Relationship
    {
        $dbQuery = (new DbQuery())
            ->setQuery('MATCH ()-[rel]->() WHERE ID(rel) = $id RETURN rel')
            ->setBind(['id' => $relationshipId]);

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

        $relationship = new Relationship();

        foreach ($qResult->records() as $record) {
            $relationship = $this->loadRelationshipFromRecord($record->get('rel'));
        }

        return $relationship;
    }


    /**
     * @param $recordRelationship
     * @return Relationship
     */
    protected function loadRelationshipFromRecord(
        \GraphAware\Neo4j\Client\Formatter\Type\Relationship $recordRelationship
    ): Relationship {
        $relationship = new Relationship();

        $sourceNode = (new Node())
            ->setUuid($this->getNodeUuidById($recordRelationship->startNodeIdentity()));

        $targetNode = (new Node())
            ->setUuid($this->getNodeUuidById($recordRelationship->endNodeIdentity()));

        $relationship->setType($recordRelationship->type());
        $relationship->setSourceNode($sourceNode);
        $relationship->setTargetNode($targetNode);

        foreach ($recordRelationship->values() as $name => $value) {
            $relationship->setProperty($this->valueToProperty($name, $value));
        }

        return $relationship;
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
            $relationship = $this->loadRelationshipFromRecord($record->get('r'));

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
            $relationship = $this->loadRelationshipFromRecord($record->get('r'));

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
     * @return array
     */
    public function listResults(DbQuery $dbQuery): array
    {
        $rows = [];
        $this->db->logQuery($dbQuery);

        try {
            $qResult = $this->db->runQuery($dbQuery);
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

        foreach ($qResult->records() as $record) {
            $row = [];

            foreach ($record->keys() as $key) {
                $value = $record->get($key);

                if (is_object($value)) {
                    if ($value instanceof \GraphAware\Neo4j\Client\Formatter\Type\Node) {
                        $row[$key] = $this->loadNodeFromRecord($value);
                    } elseif ($value instanceof \GraphAware\Neo4j\Client\Formatter\Type\Relationship) {
                        $row[$key] = $this->loadRelationshipFromRecord($value);
                    } else {
                        throw new \RuntimeException(
                            sprintf(
                                '%s: Unsupported record type <%s>.',
                                __METHOD__,
                                get_class($value)
                            )
                        );
                    }
                } else {
                    $row[$key] = $value;
                }
            }

            $rows[] = $row;
        }

        return $rows;
    }


    /**
     * @param string[] $arr
     * @return void
     */
    protected function sort(array &$arr)
    {
        if (!$this->collator) {
            // TODO: Fix hardcoded locale
            $this->collator = new \Collator('en');
        }

        usort($arr, [$this->collator, 'compare']);
    }
}