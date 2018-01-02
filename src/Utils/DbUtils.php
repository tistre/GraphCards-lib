<?php

namespace GraphCards\Utils;


class DbUtils
{
    public static function labelsString(array $labels)
    {
        $result = '';

        foreach ($labels as $label) {
            if (strlen($label) === 0) {
                continue;
            }
            
            $result .= sprintf(':`%s`', $label);
        }

        return $result;
    }


    public static function propertiesString(array $properties, &$bind)
    {
        $propertyStrings = [];

        foreach ($properties as $key => $value) {
            if (empty($value)) {
                continue;
            }

            if (is_array($value)) {
                $parts = [];

                foreach ($value as $i => $v) {
                    $bindKey = 'bind_' . $i . '_' . count($bind);
                    $parts[] = sprintf('{%s}', $bindKey);
                    $bind[$bindKey] = $v;
                }

                $propertyStrings[] = sprintf('`%s`: [ %s ]', $key, implode(', ', $parts));
            } else {
                $bindKey = 'bind_' . count($bind);
                $propertyStrings[] = sprintf('`%s`: {%s}', $key, $bindKey);
                $bind[$bindKey] = $value;
            }
        }

        return implode(', ', $propertyStrings);
    }


    public static function propertiesUpdateString($node, array $properties, &$bind)
    {
        $setPropertyStrings = [];
        $removePropertyStrings = [];

        foreach ($properties as $key => $value) {
            if ((is_array($value) && (count($value) === 0)) || ((!is_array($value)) && (strlen($value) === 0))) {
                $removePropertyStrings[] = sprintf('%s.`%s`', $node, $key);
                continue;
            }

            if (is_array($value)) {
                $parts = [];

                foreach ($value as $i => $v) {
                    $bindKey = 'bind_' . $i . '_' . count($bind);
                    $parts[] = sprintf('{%s}', $bindKey);
                    $bind[$bindKey] = $v;
                }

                $setPropertyStrings[] = sprintf('%s.`%s` = [ %s ]', $node, $key, implode(', ', $parts));
            } else {
                $bindKey = 'bind_' . count($bind);
                $setPropertyStrings[] = sprintf('%s.`%s` = {%s}', $node, $key, $bindKey);
                $bind[$bindKey] = $value;
            }
        }

        $result = '';

        if (count($removePropertyStrings) > 0) {
            $result .= sprintf(' REMOVE %s', implode(', ', $removePropertyStrings));
        }

        if (count($setPropertyStrings) > 0) {
            $result .= sprintf(' SET %s', implode(', ', $setPropertyStrings));
        }

        return $result;
    }
}