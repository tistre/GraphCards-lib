<?php

require dirname(dirname(dirname(__DIR__))) . '/autoload.php';


class ImportScript
{
    /** @var array */
    protected $options = [];

    /** @var string[] */
    protected $inputFiles = [];

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

        foreach ($this->inputFiles as $filename) {
            $this->importFile($filename);
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

        $this->inputFiles = array_slice($argv, $optind);
    }


    /**
     * @return void
     */
    protected function showHelp(): void
    {
        ?>

        Usage: php bin/import.php \
        --defaultConnection=http://neo4j:secret@localhost:7474 \
        --boltConnection=bolt://neo4j:secret@localhost:7687 \
        examples/node.xml

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
     * @param string $filename
     * @return void
     */
    protected function importFile(string $filename): void
    {
        if (!file_exists($filename)) {
            printf("File <%s> not found\n", $filename);
            return;
        }

        printf("Importing Graph XML from <%s>\n", $filename);

        $objects = new \GraphCards\Utils\XmlReader($filename);

        foreach ($objects as $object) {
            if (!is_object($object)) {
                continue;
            }

            if ($object instanceof \GraphCards\Model\Node) {
                try {
                    $node = $this->dbAdapter->createNode($object);
                    printf("Created :%s node <%s>\n", implode(':', $node->getLabels()),
                        $node->getUuid());
                } catch (\Exception $e) {
                    printf("Error creating node: %s\n", print_r($object, true));
                }
            } elseif ($object instanceof \GraphCards\Model\Relationship) {
                try {
                    $relationship = $this->dbAdapter->createRelationship($object);
                    printf("Created :%s relationship <%s>\n", $relationship->getType(),
                        $relationship->getUuid());
                } catch (\Exception $e) {
                    printf("Error creating relationship: %s\n", print_r($object, true));
                }
            }
        }
    }
}

(new ImportScript())->execute();
