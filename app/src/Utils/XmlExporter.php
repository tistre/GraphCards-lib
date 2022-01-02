<?php
declare(strict_types=1);

namespace GraphCards\Utils;

use AppBundle\ViewModel\PropertyViewModel;
use GraphCards\Model\Node;
use GraphCards\Model\Property;
use GraphCards\Model\Relationship;


class XmlExporter
{
    /** @var \XMLWriter */
    public $writer;


    /**
     * XmlExporter constructor.
     *
     * @param string
     */
    public function __construct(string $writerUri)
    {
        $this->writer = new \XMLWriter();

        if (strlen($writerUri) === 0) {
            $this->writer->openMemory();
        } else {
            $this->writer->openURI($writerUri);
        }

        $this->writer->setIndent(true);
        $this->writer->setIndentString('  ');
    }


    /**
     * @return void
     */
    public function startDocument()
    {
        $this->writer->startDocument('1.0', 'UTF-8');

        // <graph>
        $this->writer->startElement('graph');
        $this->writer->writeAttribute('xmlns', 'https://topiccards.net/GraphCards/xmlns');
    }


    /**
     * @param Node $node
     * @param array $addAttributes
     * @return void
     */
    public function exportNode(Node $node, $addAttributes = [])
    {
        // <node>
        $this->writer->startElement('node');

        foreach ($addAttributes as $name => $value) {
            $this->writer->writeAttribute($name, (string)$value);
        }

        foreach ($node->getLabels() as $label) {
            // <label></label>
            $this->writer->writeElement('label', $label);
        }

        foreach ($node->getProperties() as $property) {
            $this->exportProperty($property);
        }

        // </node>
        $this->writer->endElement();
    }


    /**
     * @param Relationship $relationship
     * @param array $addAttributes
     * @return void
     */
    public function exportRelationship(Relationship $relationship, $addAttributes = [])
    {
        // <relationship>
        $this->writer->startElement('relationship');

        foreach ($addAttributes as $name => $value) {
            $this->writer->writeAttribute($name, (string)$value);
        }

        // <type></type>
        $this->writer->writeElement('type', $relationship->getType());

        foreach ($relationship->getProperties() as $property) {
            $this->exportProperty($property);
        }

        // <source></source>
        $this->writer->startElement('source');
        $this->exportNode($relationship->getSourceNode());
        $this->writer->endElement();

        // <target></target>
        $this->writer->startElement('target');
        $this->exportNode($relationship->getTargetNode());
        $this->writer->endElement();

        // </relationship>
        $this->writer->endElement();
    }


    /**
     * @param Property $property
     * @return void
     */
    public function exportProperty(Property $property)
    {
        // <property>
        $this->writer->startElement('property');
        $this->writer->writeAttribute('key', $property->getName());

        foreach ($property->getValues() as $propertyValue) {
            // <value></value>
            $this->writer->startElement('value');
            $this->writer->writeAttribute('type', $propertyValue->getType());
            $this->writer->text((string)$propertyValue->getValue());
            $this->writer->endElement();
        }

        // </property>
        $this->writer->endElement();
    }


    /**
     * @return void
     */
    public function endDocument()
    {
        // </graph>
        $this->writer->endElement();
    }
}