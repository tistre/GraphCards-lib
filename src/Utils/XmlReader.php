<?php

namespace GraphCards\Utils;


class XmlReader implements \Iterator
{
    /** @var string */
    protected $fileName = '';

    /** @var \XMLReader */
    protected $xmlReader;

    /** @var XmlImporter */
    protected $importer;

    /** @var int */
    protected $cnt = -1;


    /**
     * GraphMlReader constructor.
     *
     * @param string $fileName
     */
    public function __construct(string $fileName)
    {
        $this->fileName = $fileName;
        $this->xmlReader = new \XMLReader();
        $this->importer = new XmlImporter();
    }


    /**
     * @return void
     */
    public function rewind()
    {
        if (!file_exists($this->fileName)) {
            return;
        }

        $ok = $this->xmlReader->open($this->fileName);

        // Go to the root node

        if ($ok) {
            $ok = $this->xmlReader->read();
        }

        if (!$ok) {
            return;
        }

        // Go to the first child node

        while (true) {
            $ok = $this->xmlReader->read();

            if (!$ok) {
                return;
            }

            if ($this->xmlReader->nodeType === \XMLReader::ELEMENT) {
                $this->cnt = 0;

                return;
            }
        }
    }


    public function current()
    {
        /** @var \DOMElement $domNode */

        $domNode = $this->xmlReader->expand();

        if ($domNode === false) {
            return false;
        }

        if ($domNode->nodeType !== XML_ELEMENT_NODE) {
            return false;
        }

        if ($domNode->tagName === 'node') {
            return $this->importer->importNode($domNode);
        } elseif ($domNode->tagName === 'relationship') {
            return $this->importer->importRelationship($domNode);
        } else {
            return false;
        }
    }


    /**
     * @return int
     */
    public function key(): int
    {
        return $this->cnt;
    }


    /**
     * @return void
     */
    public function next()
    {
        while (true) {
            $ok = $this->xmlReader->next();

            if (!$ok) {
                $this->cnt = -1;

                return;
            }

            if ($this->xmlReader->nodeType === \XMLReader::ELEMENT) {
                $this->cnt++;

                return;
            }
        }

        $this->cnt = -1;
    }


    /**
     * @return bool
     */
    public function valid(): bool
    {
        return ($this->cnt >= 0);
    }
}