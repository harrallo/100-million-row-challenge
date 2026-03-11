<?php

declare(strict_types=1);

namespace App;

final class Parser
{
    public static function parse(string $inPath, string $outPath)
    {
        \gc_disable();
        $keys = [];
        $dates = [];
        $datesCount = 0;
        for ($y = 21; $y <= 26; $y++) {
            for ($m = 1; $m <= 12; $m++) {
                $daysInMonth = match($m) {
                    2 => ($y % 4 === 0) ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default => 31
                };

                $month = $m < 10 ? "0{$m}" : (string)$m;
                $date = "{$y}-{$month}-";
                for ($d = 1; $d <= $daysInMonth; $d++) {
                    $key = $date . ($d < 10 ? "0{$d}" : (string)$d);
                    $keys[$key] = $datesCount;
                    $dates[$datesCount] = $key;
                    $datesCount++;
                }
            }
        }

        $inFile = \fopen($inPath, 'rb');
        \stream_set_read_buffer($inFile, 0);
        $data = \fread($inFile, 181000);
        $outData = [];
        $uris = [];
        $urisCount = 0;
        $lastLine = \strrpos($data, "\n") ?: 0;
        $lineNo = 0;
        while ($lineNo < $lastLine) {
            $currLine = \strpos($data, "\n", $lineNo + 52);
            if ($currLine === false) break;
            $uri = \substr($data, $lineNo + 25, $currLine - $lineNo - 51);
            if (!isset($outData[$uri])) {
                $uris[$urisCount] = $uri;
                $outData[$uri] = $urisCount * $datesCount;
                $urisCount++;
            }
            $lineNo = $currLine + 1;
        }

        unset($data);
        \fseek($inFile, 0, SEEK_END);
        $remaining = \ftell($inFile);
        $all = \array_fill(0, $urisCount * $datesCount, 0);
        $start = 0;
        \fseek($inFile, 0);
        do {
            $chunk = \fread($inFile, $remaining > 1_048_576 ? 1_048_576 : $remaining);
            $chunkLen = \strlen($chunk);
            $remaining -= $chunkLen;
            $lastRow = \strrpos($chunk, "\n");
            if ($lastRow === false) {
                break;
            }

            $finish = $chunkLen - $lastRow - 1;
            if ($finish > 0) {
                \fseek($inFile, -$finish, \SEEK_CUR);
                $remaining += $finish;
            }

            $row = 25;
            $limit = $lastRow - 1010;
            do {
                $rowEnd = \strpos($chunk, ',', $row);
                $all[$outData[\substr($chunk, $row, $rowEnd - $row)] + $keys[\substr($chunk, $rowEnd + 3, 8)]]++;
                $row = $rowEnd + 52;
                $rowEnd = \strpos($chunk, ',', $row);
                $all[$outData[\substr($chunk, $row, $rowEnd - $row)] + $keys[\substr($chunk, $rowEnd + 3, 8)]]++;
                $row = $rowEnd + 52;
                $rowEnd = \strpos($chunk, ',', $row);
                $all[$outData[\substr($chunk, $row, $rowEnd - $row)] + $keys[\substr($chunk, $rowEnd + 3, 8)]]++;
                $row = $rowEnd + 52;
                $rowEnd = \strpos($chunk, ',', $row);
                $all[$outData[\substr($chunk, $row, $rowEnd - $row)] + $keys[\substr($chunk, $rowEnd + 3, 8)]]++;
                $row = $rowEnd + 52;
                $rowEnd = \strpos($chunk, ',', $row);
                $all[$outData[\substr($chunk, $row, $rowEnd - $row)] + $keys[\substr($chunk, $rowEnd + 3, 8)]]++;
                $row = $rowEnd + 52;
                $rowEnd = \strpos($chunk, ',', $row);
                $all[$outData[\substr($chunk, $row, $rowEnd - $row)] + $keys[\substr($chunk, $rowEnd + 3, 8)]]++;
                $row = $rowEnd + 52;
                $rowEnd = \strpos($chunk, ',', $row);
                $all[$outData[\substr($chunk, $row, $rowEnd - $row)] + $keys[\substr($chunk, $rowEnd + 3, 8)]]++;
                $row = $rowEnd + 52;
                $rowEnd = \strpos($chunk, ',', $row);
                $all[$outData[\substr($chunk, $row, $rowEnd - $row)] + $keys[\substr($chunk, $rowEnd + 3, 8)]]++;
                $row = $rowEnd + 52;
                $rowEnd = \strpos($chunk, ',', $row);
                $all[$outData[\substr($chunk, $row, $rowEnd - $row)] + $keys[\substr($chunk, $rowEnd + 3, 8)]]++;
                $row = $rowEnd + 52;
                $rowEnd = \strpos($chunk, ',', $row);
                $all[$outData[\substr($chunk, $row, $rowEnd - $row)] + $keys[\substr($chunk, $rowEnd + 3, 8)]]++;
                $row = $rowEnd + 52;
            } while ($row < $limit);

            while ($row < $lastRow) {
                $rowEnd = \strpos($chunk, ',', $row);
                if ($rowEnd === false || $rowEnd >= $lastRow) {
                    break;
                }

                $all[$outData[\substr($chunk, $row, $rowEnd - $row)] + $keys[\substr($chunk, $rowEnd + 3, 8)]]++;
                $row = $rowEnd + 52;
            }
        } while($remaining > 0);
        
        \fclose($inFile);
        $outFile = \fopen($outPath, 'wb');
        \stream_set_write_buffer($outFile, 1_048_576);
        \fwrite($outFile, '{');
        $first = '';
        for ($id = 0; $id < $urisCount; $id++) {
            $k = $id * $datesCount;
            $sums = [];
            for ($dateId = 0; $dateId < $datesCount; $dateId++) {
                $sum = $all[$k + $dateId];
                if ($sum === 0) {
                    continue;
                }

                $sums[] = '        "20' . $dates[$dateId] . '": ' . $sum;
            }

            if ($sums === []) {
                continue;
            }

            \fwrite($outFile, $first . "\n    \"\\/blog\\/" . str_replace('/', '\\/', $uris[$id]) . "\": {\n" . \implode(",\n", $sums) . "\n    }");
            $first = ',';
        }

        \fwrite($outFile, "\n}");
        \fclose($outFile);
    }
}
