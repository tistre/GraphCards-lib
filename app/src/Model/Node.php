<?php

namespace GraphCards\Model;


class Node
{
    /** @var string[] */
    protected array $labels = [];

    /** @var Property[] */
    protected array $properties = [];


    /**
     * @return string
     */
    public function getUuid(): string
    {
        if (!$this->hasProperty('uuid')) {
            return '';
        }

        return (string)$this->getProperty('uuid')->getFirstValue()->getValue();
    }


    /**
     * @param string $uuid
     * @return self
     */
    public function setUuid(string $uuid): self
    {
        $property = (new Property())
            ->setName('uuid')
            ->addValue((new PropertyValue())->setValue($uuid));

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
     * @param string $name
     * @return bool
     */
    public function hasProperty(string $name): bool
    {
        return isset($this->properties[$name]);
    }


    /**
     * @param string $name
     * @return Property
     */
    public function getProperty(string $name): Property
    {
        if (!$this->hasProperty($name)) {
            throw new \RuntimeException(sprintf('%s: Property "%s" not set.', __METHOD__, $name));
        }

        return $this->properties[$name];
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
