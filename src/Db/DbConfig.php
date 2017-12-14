<?php

namespace GraphCards\Db;


class DbConfig
{
    /**
     * @var string Example: "http://neo4j:password@localhost:7474"
     */
    protected $defaultConnection = '';

    /**
     * @var string Example: "bolt://neo4j:password@localhost:7687"
     */
    protected $boltConnection = '';


    /**
     * @return string
     */
    public function getDefaultConnection(): string
    {
        return $this->defaultConnection;
    }


    /**
     * @param string $defaultConnection
     * @return self
     */
    public function setDefaultConnection(string $defaultConnection): self
    {
        $this->defaultConnection = $defaultConnection;
        return $this;
    }


    /**
     * @return string
     */
    public function getBoltConnection(): string
    {
        return $this->boltConnection;
    }


    /**
     * @param string $boltConnection
     * @return self
     */
    public function setBoltConnection(string $boltConnection): self
    {
        $this->boltConnection = $boltConnection;
        return $this;
    }
}