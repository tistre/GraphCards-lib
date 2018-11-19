<?php
declare(strict_types=1);

namespace GraphCards\Model;


class Property
{
    /** @var string */
    protected $name;

    /** @var PropertyValue[] */
    protected $values = [];


    /**
     * @return string
     */
    public function getName(): string
    {
        return $this->name;
    }


    /**
     * @param string $name
     * @return self
     */
    public function setName(string $name): self
    {
        $this->name = $name;
        return $this;
    }


    /**
     * @return PropertyValue[]
     */
    public function getValues(): array
    {
        return $this->values;
    }


    /**
     * @param PropertyValue[] $propertyValues
     * @return self
     */
    public function setValues(array $propertyValues): self
    {
        $this->values = [];

        foreach ($propertyValues as $propertyValue) {
            $this->addValue($propertyValue);
        }

        return $this;
    }


    /**
     * @return PropertyValue
     */
    public function getFirstValue(): PropertyValue
    {
        if (count($this->values) === 0) {
            return new PropertyValue();
        }

        return $this->values[0];
    }


    /**
     * @param PropertyValue $propertyValue
     * @return self
     */
    public function addValue(PropertyValue $propertyValue): self
    {
        if (strlen(trim($propertyValue->getValue())) > 0) {
            $this->values[] = $propertyValue;
        }

        return $this;
    }
}
