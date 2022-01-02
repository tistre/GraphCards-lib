<?php
declare(strict_types=1);

namespace GraphCards\Utils;

use GraphCards\Model\Node;
use GraphCards\Model\Property;
use GraphCards\Model\PropertyValue;
use GraphCards\Model\Relationship;


class XmlImporter
{
    /**
     * @param \DOMElement $domNode
     * @return Node
     */
    public function importNode(\DOMElement $domNode): Node
    {
        // TODO: How to update ("upsert")?

        /*
        Example:

        <graph xmlns="https://topiccards.net/GraphCards/xmlns">
          <node>
            <label>LABEL1</label>
            <label>LABEL2</label>
            <property>…</property>
          </node>
        </graph>
        */

        $node = new Node();

        // Labels

        /** @var \DOMElement $domSubNode */
        foreach ($this->getChildrenByTagName($domNode,'label') as $domSubNode) {
            $label = $domSubNode->nodeValue;

            if (strlen($label) === 0) {
                continue;
            }

            $node->setLabels(array_merge($node->getLabels(), [$label]));
        }

        // Properties

        foreach ($this->getChildrenByTagName($domNode,'property') as $domSubNode) {
            $property = $this->importProperty($domSubNode);

            if ((strlen($property->getName()) === 0) || (count($property->getValues()) === 0)) {
                continue;
            }

            $node->setProperty($property);
        }

        return $node;
    }


    /**
     * @param \DOMElement $domNode
     * @return Relationship
     */
    public function importRelationship(\DOMElement $domNode): Relationship
    {
        /*
        Example:

        <graph xmlns="https://topiccards.net/GraphCards/xmlns">
          <relationship>
            <type>TYPE</type>
            <property>…</property>
            <source>
              <node>…</node>
            </source>
            <target>
              <node>…</node>
            </target>
          </relationship>
        </graph>
        */

        $relationship = new Relationship();

        // Type

        /** @var \DOMElement $domSubNode */
        foreach ($this->getChildrenByTagName($domNode,'type') as $domSubNode) {
            $relationship->setType($domSubNode->nodeValue);
        }

        // Properties

        foreach ($this->getChildrenByTagName($domNode,'property') as $domSubNode) {
            $property = $this->importProperty($domSubNode);

            if ((strlen($property->getName()) === 0) || (count($property->getValues()) === 0)) {
                continue;
            }

            $relationship->setProperty($property);
        }

        // Source

        foreach ($this->getChildrenByTagName($domNode, 'source') as $domSubNode) {
            foreach ($this->getChildrenByTagName($domSubNode,'node') as $nodeNode) {
                $relationship->setSourceNode($this->importNode($nodeNode));
            }
        }

        // Target

        foreach ($this->getChildrenByTagName($domNode,'target') as $domSubNode) {
            foreach ($this->getChildrenByTagName($domSubNode,'node') as $nodeNode) {
                $relationship->setTargetNode($this->importNode($nodeNode));
            }
        }

        return $relationship;
    }


    /**
     * @param \DOMElement $domNode
     * @return Property
     */
    protected function importProperty(\DOMElement $domNode): Property
    {
        /*
        Example:

        <property key="KEY">
          <value type="string">VALUE</value>
        </property>
        */

        $property = new Property();

        if ($domNode->hasAttribute('key')) {
            $property->setName($domNode->getAttribute('key'));
        }

        foreach ($this->getChildrenByTagName($domNode,'value') as $domSubNode) {
            $propertyValue = new PropertyValue();

            if ($domSubNode->hasAttribute('type')) {
                $propertyValue->setType($domSubNode->getAttribute('type'));
            }

            $propertyValue->setValue($domSubNode->nodeValue);
            $property->addValue($propertyValue);
        }

        return $property;
    }


    /**
     * @param \DOMElement $domNode
     * @param string $tagName
     * @return \DOMElement[]
     */
    protected function getChildrenByTagName(\DOMElement $domNode, string $tagName): array
    {
        $result = [];

        foreach ($domNode->childNodes as $childNode) {
            if (($childNode instanceof \DOMElement) && ($childNode->tagName === $tagName)) {
                $result[] = $childNode;
            }
        }

        return $result;
    }
}