<?php
declare(strict_types=1);

namespace GraphCards\Db;

use GraphAware\Common\Result\Result;


class DbResult
{
    /** @var DbAdapter */
    protected $dbAdapter;

    /** @var DbQuery */
    protected $dbQuery;

    /** @var Result */
    protected $neo4jResult;


    /**
     * DbResult constructor.
     * @param Result $result
     */
    public function __construct(DbAdapter $dbAdapter, DbQuery $dbQuery, Result $neo4jResult)
    {
        $this->dbAdapter = $dbAdapter;
        $this->dbQuery = $dbQuery;
        $this->neo4jResult = $neo4jResult;
    }


    /**
     * @return DbAdapter
     */
    public function getDbAdapter(): DbAdapter
    {
        return $this->dbAdapter;
    }


    /**
     * @return int
     */
    public function getRecordCount(): int
    {
        return $this->neo4jResult->size();
    }


    /**
     * @return bool
     */
    public function isEmpty(): bool
    {
        return ($this->neo4jResult->size() < 1);
    }


    /**
     * @return DbRecord|null
     */
    public function getFirstRecord(): ?DbRecord
    {
        if ($this->isEmpty()) {
            return null;
        }

        return new DbRecord($this, $this->neo4jResult->firstRecord());
    }


    /**
     * @return DbRecord[]
     */
    public function getRecords(): array
    {
        $result = [];

        foreach ($this->neo4jResult->records() as $neo4jRecord) {
            $result[] = new DbRecord($this, $neo4jRecord);
        }

        return $result;
    }
}