<?php

namespace GraphCards\Utils;


class DbUtils
{
    /**
     * @param string[] $labels
     * @return string
     */
    public static function labelsString(array $labels): string
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


    /**
     * @param array $properties
     * @param array $bind
     * @return string
     */
    public static function propertiesString(array $properties, array &$bind): string
    {
        $propertyStrings = [];

        foreach ($properties as $key => $value) {
            if (is_array($value) && (count($value) === 1)) {
                $value = array_shift($value);
            }

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


    /**
     * @param string $node
     * @param array $properties
     * @param array $bind
     * @return string
     */
    public static function propertiesUpdateString(string $node, array $properties, array &$bind): string
    {
        $setPropertyStrings = [];
        $removePropertyStrings = [];

        foreach ($properties as $key => $value) {
            if (is_array($value) && (count($value) === 1)) {
                $value = array_shift($value);
            }

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


    /**
     * Generate UUID v4
     *
     * @see http://stackoverflow.com/questions/2040240/php-function-to-generate-v4-uuid/2040279#2040279
     * @return string
     */
    public static function generateUuid(): string
    {
        return sprintf
        (
            '%04x%04x-%04x-%04x-%04x-%04x%04x%04x',

            // 32 bits for "time_low"
            mt_rand(0, 0xffff), mt_rand(0, 0xffff),

            // 16 bits for "time_mid"
            mt_rand(0, 0xffff),

            // 16 bits for "time_hi_and_version",
            // four most significant bits holds version number 4
            mt_rand(0, 0x0fff) | 0x4000,

            // 16 bits, 8 bits for "clk_seq_hi_res",
            // 8 bits for "clk_seq_low",
            // two most significant bits holds zero and one for variant DCE1.1
            mt_rand(0, 0x3fff) | 0x8000,

            // 48 bits for "node"
            mt_rand(0, 0xffff), mt_rand(0, 0xffff), mt_rand(0, 0xffff)
        );
    }
}