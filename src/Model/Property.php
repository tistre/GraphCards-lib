<?php

namespace GraphCards\Model;


class Property
{
    /** @var string */
    protected $name;

    /** @var string */
    protected $value;


    /**
     * @return string
     */
    public function getName()
    {
        return $this->name;
    }


    /**
     * @param string $name
     * @return self
     */
    public function setName($name)
    {
        $this->name = $name;
        return $this;
    }


    /**
     * @return string
     */
    public function getValue()
    {
        return $this->value;
    }


    /**
     * @param string $value
     * @return self
     */
    public function setValue($value)
    {
        $this->value = $value;
        return $this;
    }
}
