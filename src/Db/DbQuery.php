<?php

namespace GraphCards\Db;


class DbQuery
{
    /** @var string */
    public $query = '';

    /** @var array */
    public $bind = [];


    /**
     * @param string $query
     * @return self
     */
    public function setQuery(string $query): DbQuery
    {
        $this->query = $query;
        return $this;
    }


    /**
     * @param array $bind
     * @return self
     */
    public function setBind(array $bind): DbQuery
    {
        $this->bind = $bind;
        return $this;
    }


    /**
     * @param string $key
     * @param string $value
     * @return self
     */
    public function addBind(string $key, string $value): DbQuery
    {
        $this->bind[$key] = $value;
    }
}