<?php

require dirname(dirname(dirname(__DIR__))) . '/autoload.php';


class ExportScript
{
    /** @var array */
    protected $options = [];

    /** @var string[] */
    protected $queries = [];

    /** @var \Psr\Log\LoggerInterface */
    protected $logger;

    /** @var \GraphCards\Db\DbConfig */
    protected $dbConfig;

    /** @var \GraphCards\Db\Db */
    protected $db;

    /** @var \GraphCards\Db\DbAdapter */
    protected $dbAdapter;


    /**
     * @return void
     */
    public function execute(): void
    {
        $this->getOptions();

        if (isset($this->options['h'])) {
            $this->showHelp();
            exit;
        }

        $this->logger = (new \Monolog\Logger('log'))->pushHandler(new \Monolog\Handler\ErrorLogHandler());

        $this->initDb();

        foreach ($this->queries as $query) {
            $this->exportQuery($query);
        }
    }


    /**
     * @return void
     */
    protected function getOptions(): void
    {
        global $argv;

        $shortopts = 'h';

        $longopts = [
            'defaultConnection:',
            'boltConnection:'
        ];

        $this->options = getopt($shortopts, $longopts, $optind);

        $this->queries = array_slice($argv, $optind);
    }


    /**
     * @return void
     */
    protected function showHelp(): void
    {
        ?>

        Usage:

        php bin/export.php \
        --defaultConnection=http://neo4j:secret@localhost:7474 \
        --boltConnection=bolt://neo4j:secret@localhost:7687 \
        'MATCH (node) RETURN node LIMIT 10'

        <?php
    }


    /**
     * @return void
     */
    protected function initDb(): void
    {
        $this->dbConfig = (new \GraphCards\Db\DbConfig())
            ->setDefaultConnection($this->options['defaultConnection'])
            ->setBoltConnection($this->options['boltConnection'])
            ->setLogger($this->logger);

        $this->db = new \GraphCards\Db\Db($this->dbConfig);
        $this->dbAdapter = new \GraphCards\Db\DbAdapter($this->db);
    }


    /**
     * @param string $query
     * @return void
     */
    protected function exportQuery(string $query): void
    {
        $dbQuery = (new \GraphCards\Db\DbQuery())
            ->setQuery($query);

        $xmlExporter = new \GraphCards\Utils\XmlExporter('php://output');
        $xmlExporter->startDocument();

        foreach ($this->dbAdapter->listResults($dbQuery) as $rowNum => $row) {
            $rowData = [];

            foreach ($row as $columnName => $obj) {
                if (is_object($obj)) {
                    if ($obj instanceof \GraphCards\Model\Node) {
                        $xmlExporter->exportNode($obj, ['rowNumber' => $rowNum, 'columnName' => $columnName]);
                    } elseif ($obj instanceof \GraphCards\Model\Relationship) {
                        $xmlExporter->exportRelationship($obj, ['rowNumber' => $rowNum, 'columnName' => $columnName]);
                    } else {
                        throw new \RuntimeException(
                            sprintf(
                                '%s: Unsupported object type <%s>.',
                                __METHOD__,
                                get_class($obj)
                            )
                        );
                    }
                } else {
                    $rowData[$columnName] = (string)$obj;
                }
            }

            if (count($rowData) > 0) {
                // <row>
                $xmlExporter->writer->startElement('row');
                $xmlExporter->writer->writeAttribute('rowNumber', $rowNum);

                foreach ($rowData as $columnName => $value) {
                    // <record></record>
                    $xmlExporter->writer->startElement('record');
                    $xmlExporter->writer->writeAttribute('columnName', $columnName);
                    $xmlExporter->writer->text($value);
                    $xmlExporter->writer->endElement();
                }

                // </row>
                $xmlExporter->writer->endElement();
            }
        }

        $xmlExporter->endDocument();
    }
}

(new ExportScript())->execute();
