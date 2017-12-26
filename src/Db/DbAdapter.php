<?php

namespace GraphCards\Db;

use GraphCards\Model\Node;
use GraphCards\Model\Property;
use GraphCards\Model\Relationship;
use GraphCards\Utils\DbUtils;
use GraphAware\Neo4j\Client\Exception\Neo4jExceptionInterface;
use GraphAware\Neo4j\Client\Result\ResultCollection;


class DbAdapter
{
    /** @var Db */
    protected $db;


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
            $propertyData[$property->getName()] = $property->getValue();
        }

        $bind = [];
        $propertyQuery = DbUtils::propertiesString($propertyData, $bind);

        $query = sprintf
        (
            'CREATE (n%s { %s }) RETURN ID(n)',
            DbUtils::labelsString($node->getLabels()),
            $propertyQuery
        );

        $transaction = $this->db->beginTransaction();
        $transaction->push($query, $bind);
        $this->db->logQuery($query, $bind);

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

            $oldProperties[$property->getName()] = $property->getValue();
        }

        $newProperties = [];

        foreach ($newNode->getProperties() as $property) {
            if ($property->getName() === 'uuid') {
                continue;
            }

            if (strlen(trim($property->getValue())) === 0) {
                continue;
            }

            $newProperties[$property->getName()] = $property->getValue();
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

        $bind = ['uuid' => $newNode->getUuid()];
        $propertyQuery = DbUtils::propertiesUpdateString('node', $updatedProperties, $bind);

        $query = sprintf
        (
            'MATCH (node { uuid: {uuid} })%s',
            $propertyQuery
        );

        if (count($removedLabels) > 0) {
            $query .= sprintf
            (
                ' REMOVE node%s',
                DbUtils::labelsString($removedLabels)
            );
        }

        if (count($addedLabels) > 0) {
            $query .= sprintf
            (
                ' SET node%s',
                DbUtils::labelsString($addedLabels)
            );
        }

        $transaction->push($query, $bind);
        $this->db->logQuery($query, $bind);

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
        $query = 'MATCH (node) WHERE ID(node) = $id RETURN node';
        $bind = ['id' => $nodeId];
        $this->db->logQuery($query, $bind);

        try {
            $qResult = $this->db->getConnection()->run($query, $bind);
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
        $query = 'MATCH (node) WHERE ID(node) = $id RETURN node.uuid';
        $bind = ['id' => $nodeId];
        $this->db->logQuery($query, $bind);

        try {
            $qResult = $this->db->getConnection()->run($query, $bind);
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
        $query = 'MATCH (node { uuid: {uuid} }) RETURN node';
        $bind = ['uuid' => $nodeUuid];
        $this->db->logQuery($query, $bind);

        try {
            $qResult = $this->db->getConnection()->run($query, $bind);
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
     * @param int|float|bool|string $value
     * @return Property
     */
    protected function valueToProperty(string $name, $value): Property
    {
        if (is_int($value)) {
            $type = Property::TYPE_INTEGER;
        } elseif (is_float($value)) {
            $type = Property::TYPE_FLOAT;
        } elseif (is_bool($value)) {
            $type = Property::TYPE_BOOLEAN;
        } else {
            $type = Property::TYPE_STRING;
        }

        return (new Property())
            ->setName($name)
            ->setType($type)
            ->setValue($value);
    }


    /**
     * @param string $nodeUuid
     * @return void
     */
    public function deleteNode($nodeUuid)
    {
        $query = 'MATCH (node { uuid: {uuid} }) DELETE node';
        $bind = ['uuid' => $nodeUuid];
        $this->db->logQuery($query, $bind);

        try {
            $this->db->getConnection()->run($query, $bind);
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
     * @param string $label
     * @return Node[]
     */
    public function listNodes(string $label = ''): array
    {
        $nodes = [];

        $query = sprintf(
            'MATCH (node%s) RETURN node LIMIT 20',
            DbUtils::labelsString([$label])
        );

        $bind = [];
        $this->db->logQuery($query, $bind);

        try {
            $qResult = $this->db->getConnection()->run($query, $bind);
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
     * @param string $relationshipUuid
     * @return Relationship
     */
    public function loadRelationship($relationshipUuid): Relationship
    {
        $query = 'MATCH ()-[relationship { uuid: {uuid} }]->() RETURN relationship';
        $bind = ['uuid' => $relationshipUuid];
        $this->db->logQuery($query, $bind);

        try {
            $qResult = $this->db->getConnection()->run($query, $bind);
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
        $query = 'MATCH ()-[rel]->() WHERE ID(rel) = $id RETURN rel';
        $bind = ['id' => $relationshipId];
        $this->db->logQuery($query, $bind);

        try {
            $qResult = $this->db->getConnection()->run($query, $bind);
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
     * @param int $limit
     * @return Relationship[]
     */
    public function listRelationships(int $limit): array
    {
        $relationships = [];

        $query = 'MATCH (n1)-[r]->(n2) RETURN r LIMIT ' . $limit;
        $bind = [];
        $this->db->logQuery($query, $bind);

        try {
            $qResult = $this->db->getConnection()->run($query, $bind);
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
     * @param string $nodeUuid
     * @return Relationship[]
     */
    public function listNodeRelationships(string $nodeUuid): array
    {
        $relationships = [];

        $query = 'MATCH (n1 {uuid: {n1uuid}})-[r]-(n2) RETURN r LIMIT 20';
        $bind = ['n1uuid' => $nodeUuid];
        $this->db->logQuery($query, $bind);

        try {
            $qResult = $this->db->getConnection()->run($query, $bind);
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
     * @param Property[] $properties
     * @param array $bind
     * @return string
     */
    protected function propertiesString(array $properties, &$bind): string
    {
        $propertyData = [];

        foreach ($properties as $property) {
            $propertyData[$property->getName()] = $property->getValue();
        }

        return DbUtils::propertiesString($propertyData, $bind);
    }


    /**
     * @param Relationship $relationship
     * @return Relationship
     */
    public function createRelationship(Relationship $relationship): Relationship
    {
        $bind = [];

        $query = sprintf
        (
            'MATCH (s%s {%s}), (t%s {%s}) MERGE (s)-[r%s {%s}]->(t) RETURN ID(r)',
            DbUtils::labelsString($relationship->getSourceNode()->getLabels()),
            $this->propertiesString($relationship->getSourceNode()->getProperties(), $bind),
            DbUtils::labelsString($relationship->getTargetNode()->getLabels()),
            $this->propertiesString($relationship->getTargetNode()->getProperties(), $bind),
            DbUtils::labelsString([$relationship->getType()]),
            $this->propertiesString($relationship->getProperties(), $bind)
        );

        $transaction = $this->db->beginTransaction();
        $transaction->push($query, $bind);
        $this->db->logQuery($query, $bind);

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

            $oldProperties[$property->getName()] = $property->getValue();
        }

        $newProperties = [];

        foreach ($newRelationship->getProperties() as $property) {
            if ($property->getName() === 'uuid') {
                continue;
            }

            if (strlen(trim($property->getValue())) === 0) {
                continue;
            }

            $newProperties[$property->getName()] = $property->getValue();
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

        $bind = ['uuid' => $newRelationship->getUuid()];
        $propertyQuery = DbUtils::propertiesUpdateString('relationship', $updatedProperties, $bind);

        $query = sprintf
        (
            'MATCH ()-[relationship { uuid: {uuid} }]->()%s',
            $propertyQuery
        );

        $transaction->push($query, $bind);
        $this->db->logQuery($query, $bind);

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
}