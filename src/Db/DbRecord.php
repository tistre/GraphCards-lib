<?php
declare(strict_types=1);

namespace GraphCards\Db;

use GraphAware\Common\Result\Record;
use GraphCards\Model\Node;
use GraphCards\Model\Relationship;
use GraphCards\Utils\DbUtils;


class DbRecord
{
    /** @var DbResult */
    protected $dbResult;

    /** @var Record */
    protected $neo4jRecord;

    /** @var array */
    protected $allRecords = [];


    /**
     * DbRecord constructor.
     * @param DbResult $dbResult
     * @param Record $record
     */
    public function __construct(DbResult $dbResult, Record $neo4jRecord)
    {
        $this->dbResult = $dbResult;
        $this->neo4jRecord = $neo4jRecord;
    }


    /**
     * @param string $key
     * @return Node|null
     */
    public function getNode(string $key): ?Node
    {
        $neo4jNode = $this->neo4jRecord->get($key);

        if ($neo4jNode === null) {
            return null;
        }

        if (!(is_object($neo4jNode)) && ($neo4jNode instanceof \GraphAware\Neo4j\Client\Formatter\Type\Node)) {
            throw new \RuntimeException(
                sprintf(
                    '%s: Value for key <%s> is not a node object.',
                    __METHOD__,
                    $key
                )
            );
        }

        return $this->nodeFromNeo4jNode($neo4jNode);
    }


    /**
     * @return Node[]
     */
    public function getAllNodes(): array
    {
        $this->readAllRecords();

        $result = [];

        foreach ($this->allRecords['node'] as $neo4jNode) {
            $result[] = $this->nodeFromNeo4jNode($neo4jNode);
        }

        return $result;
    }


    /**
     * @param \GraphAware\Neo4j\Client\Formatter\Type\Node $neo4jNode
     * @return Node
     */
    public function nodeFromNeo4jNode(\GraphAware\Neo4j\Client\Formatter\Type\Node $neo4jNode): Node
    {
        $node = new Node();

        $node->setLabels($neo4jNode->labels());

        foreach ($neo4jNode->values() as $name => $value) {
            $node->setProperty(DbUtils::propertyFromValue($name, $value));
        }

        return $node;
    }


    /**
     * @param string $key
     * @return Relationship|null
     */
    public function getRelationship(string $key): ?Relationship
    {
        $neo4jRelationship = $this->neo4jRecord->get($key);

        if ($neo4jRelationship === null) {
            return null;
        }

        if (!(is_object($neo4jRelationship)) && ($neo4jRelationship instanceof \GraphAware\Neo4j\Client\Formatter\Type\Relationship)) {
            throw new \RuntimeException(
                sprintf(
                    '%s: Value for key <%s> is not a relationship object.',
                    __METHOD__,
                    $key
                )
            );
        }

        return $this->relationshipFromNeo4jRelationship($neo4jRelationship);
    }


    /**
     * @return Relationship[]
     */
    public function getAllRelationships(): array
    {
        $this->readAllRecords();

        $result = [];

        foreach ($this->allRecords['relationship'] as $neo4jRelationship) {
            $result[] = $this->relationshipFromNeo4jRelationship($neo4jRelationship);
        }

        return $result;
    }


    /**
     * @param \GraphAware\Neo4j\Client\Formatter\Type\Relationship $neo4jRelationship
     * @return Relationship
     */
    public function relationshipFromNeo4jRelationship(\GraphAware\Neo4j\Client\Formatter\Type\Relationship $neo4jRelationship
    ): Relationship {
        $relationship = new Relationship();

        $sourceNode = (new Node())
            ->setUuid($this->dbResult->getDbAdapter()->getNodeUuidById($neo4jRelationship->startNodeIdentity()));
        $targetNode = (new Node())
            ->setUuid($this->dbResult->getDbAdapter()->getNodeUuidById($neo4jRelationship->endNodeIdentity()));

        $relationship->setType($neo4jRelationship->type());
        $relationship->setSourceNode($sourceNode);
        $relationship->setTargetNode($targetNode);

        foreach ($neo4jRelationship->values() as $name => $value) {
            $relationship->setProperty(DbUtils::propertyFromValue($name, $value));
        }

        return $relationship;
    }


    /**
     * @param string $key
     * @return mixed
     */
    public function getValue(string $key)
    {
        $this->readAllRecords();

        if (isset($this->allRecords['value'][$key])) {
            return $this->allRecords['value'][$key];
        }

        return null;
    }


    /**
     * @return string[]
     */
    public function getAllValues(): array
    {
        $this->readAllRecords();

        return $this->allRecords['value'];
    }


    /**
     * @return void
     */
    protected function readAllRecords(): void
    {
        $this->allRecords = [
            'node' => [],
            'relationship' => [],
            'value' => []
        ];

        foreach ($this->neo4jRecord->keys() as $key) {
            $value = $this->neo4jRecord->get($key);

            if (is_object($value)) {
                if ($value instanceof \GraphAware\Neo4j\Client\Formatter\Type\Node) {
                    $this->allRecords['node'][$key] = $value;
                } elseif ($value instanceof \GraphAware\Neo4j\Client\Formatter\Type\Relationship) {
                    $this->allRecords['relationship'][$key] = $value;
                } else {
                    throw new \RuntimeException(
                        sprintf(
                            '%s: Unsupported object type <%s>.',
                            __METHOD__,
                            get_class($value)
                        )
                    );
                }
            } else {
                $this->allRecords['value'][$key] = (string)$value;
            }
        }
    }
}