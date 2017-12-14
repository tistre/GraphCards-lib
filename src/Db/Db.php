<?php

namespace GraphCards\Db;

use GraphAware\Common\Transaction\TransactionInterface;
use GraphAware\Neo4j\Client\ClientBuilder;
use GraphAware\Neo4j\Client\ClientInterface;
use GraphAware\Neo4j\Client\Transaction\Transaction;


class Db
{
    /** @var DbConfig */
    protected $dbConfig;

    /** @var ClientInterface */
    protected $connection;

    /** @var int */
    protected $transactionLevel = 0;

    /** @var TransactionInterface */
    protected $transaction;


    public function __construct(DbConfig $dbConfig)
    {
        $this->dbConfig = $dbConfig;
    }


    /**
     * @return ClientInterface
     */
    public function getConnection(): ClientInterface
    {
        if (!$this->connection) {
            $this->connection = ClientBuilder::create()
                ->addConnection('default', $this->dbConfig->getDefaultConnection())
                ->addConnection('bolt', $this->dbConfig->getBoltConnection())
                ->build();
        }

        return $this->connection;
    }


    /**
     * @return Transaction
     */
    public function beginTransaction(): Transaction
    {
        // Wrapping Neo4j driver transaction functionality because it
        // doesn't support nested transactions

        $this->transactionLevel++;

        if ($this->transactionLevel === 1) {
            $this->getConnection();

            $this->transaction = $this->connection->transaction();
        }

        return $this->transaction;
    }


    /**
     * @param Transaction $transaction
     * @return mixed
     */
    public function commit(Transaction $transaction)
    {
        if ($this->transactionLevel <= 0) {
            throw new \RuntimeException
            (
                sprintf
                (
                    '%s: Transaction level is less than zero (%s).',
                    __METHOD__, $this->transactionLevel
                )
            );
        }

        $this->transactionLevel--;

        if ($this->transactionLevel > 0) {
            return null;
        }

        // We intentionally don't reset $this->transaction here
        // since the caller might still want to read from the transaction

        return $transaction->commit();
    }


    /**
     * @param Transaction $transaction
     * @return void
     */
    public function rollBack(Transaction $transaction)
    {
        if ($this->transactionLevel <= 0) {
            throw new \RuntimeException
            (
                sprintf
                (
                    '%s: Transaction level is less than zero (%s).',
                    __METHOD__, $this->transactionLevel
                )
            );
        }

        $transaction->rollback();
        $this->transactionLevel = 0;

        // Running into "RuntimeException: A transaction is already bound to this session",
        // trying to work around it by reconnecting

        $this->connection = false;
        $this->getConnection();
    }
}