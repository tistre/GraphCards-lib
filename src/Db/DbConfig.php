<?php
declare(strict_types=1);

namespace GraphCards\Db;


use Psr\Log\LoggerInterface;

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

    /** @var LoggerInterface */
    protected $logger;


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


    /**
     * @return LoggerInterface
     */
    public function getLogger(): LoggerInterface
    {
        return $this->logger;
    }


    /**
     * @param LoggerInterface $logger
     * @return self
     */
    public function setLogger(LoggerInterface $logger): self
    {
        $this->logger = $logger;
        return $this;
    }
}