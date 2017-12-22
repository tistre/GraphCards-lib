<?php

namespace GraphCards\Model;


class Property
{
    const TYPE_INTEGER = 'integer';
    const TYPE_FLOAT = 'float';
    const TYPE_STRING = 'string';
    const TYPE_BOOLEAN = 'boolean';

    protected $types = [
        self::TYPE_INTEGER,
        self::TYPE_FLOAT,
        self::TYPE_STRING,
        self::TYPE_BOOLEAN
    ];

    /** @var string */
    protected $name;

    /** @var int|float|bool|string */
    protected $value;

    /** @var string */
    protected $type = self::TYPE_STRING;


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
     * @return int|float|bool|string
     */
    public function getValue()
    {
        return $this->value;
    }


    /**
     * @param int|float|bool|string $value
     * @return self
     */
    public function setValue($value): self
    {
        $this->value = $this->convertValueToType($value);
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
     * @return Property
     */
    public function setType(string $type): self
    {
        $type = strtolower($type);

        if (! in_array($type, $this->types)) {
            throw new \RuntimeException(sprintf('%s: Unknown type "%s".', __METHOD__, $type));
        }

        $this->type = $type;
        $this->value = $this->convertValueToType($this->value);
        return $this;
    }


    /**
     * @param int|float|bool|string $value
     * @return int|float|bool|string
     */
    protected function convertValueToType($value)
    {
        $result = $value;

        // TODO: Would it be better to throw an exception on lossy conversions?

        if ($this->type === self::TYPE_INTEGER) {
            $result = intval($value);
        } elseif ($this->type === self::TYPE_FLOAT) {
            $result = floatval($value);
        } elseif ($this->type === self::TYPE_BOOLEAN) {
            $result = boolval($value);
        } else {
            $result = (string) $value;
        }

        return $result;
    }
}
