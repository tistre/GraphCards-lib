<?php

namespace GraphCards\Model;


class Node
{
    /** @var string[] */
    protected $labels = [];

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

        return (string) $this->properties['uuid']->getValue();
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
     * @return string[]
     */
    public function getLabels(): array
    {
        return $this->labels;
    }


    /**
     * @param string[] $labels
     * @return self
     */
    public function setLabels(array $labels): self
    {
        $this->labels = array_filter($labels);
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
        if (strlen($property->getName()) === 0) {
            throw new \RuntimeException(__METHOD__ . ': Property name must not be empty.');
        }

        $this->properties[$property->getName()] = $property;

        return $this;
    }
}
