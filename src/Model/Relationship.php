<?php

namespace GraphCards\Model;


class Relationship
{
    /** @var Node */
    protected $sourceNode;

    /** @var Node */
    protected $targetNode;

    /** @var string */
    protected $type;

    /** @var Property[] */
    protected $properties = [];


    /**
     * @return string
     */
    public function getUuid(): string
    {
        if (! isset($this->properties['uuid'])) {
            return '';
        }

        return $this->properties['uuid']->getFirstValue()->getValue();
    }


    /**
     * @param string $uuid
     * @return self
     */
    public function setUuid(string $uuid): self
    {
        $property = (new Property())
            ->setName('uuid')
            ->setValue($uuid);

        return $this->setProperty($property);
    }


    /**
     * @return Node
     */
    public function getSourceNode(): Node
    {
        return $this->sourceNode;
    }


    /**
     * @param Node $sourceNode
     * @return self
     */
    public function setSourceNode(Node $sourceNode): self
    {
        $this->sourceNode = $sourceNode;
        return $this;
    }


    /**
     * @return Node
     */
    public function getTargetNode(): Node
    {
        return $this->targetNode;
    }


    /**
     * @param Node $targetNode
     * @return self
     */
    public function setTargetNode(Node $targetNode): self
    {
        $this->targetNode = $targetNode;
        return $this;
    }


    /**
     * @return string
     */
    public function getType(): string
    {
        return $this->type;
    }


    /**
     * @param string $type
     * @return self
     */
    public function setType(string $type): self
    {
        $this->type = $type;
        return $this;
    }


    /**
     * @return Property[]
     */
    public function getProperties(): array
    {
        return $this->properties;
    }


    /**
     * @param Property[] $properties
     * @return self
     */
    public function setProperties(array $properties): self
    {
        $this->properties = [];

        foreach ($properties as $property) {
            $this->setProperty($property);
        }

        return $this;
    }


    /**
     * @param Property $property
     * @return self
     */
    public function setProperty(Property $property): self
    {
        $this->properties[$property->getName()] = $property;
        return $this;
    }
}
