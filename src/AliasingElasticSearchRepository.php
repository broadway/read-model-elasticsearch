<?php

declare(strict_types=1);

/*
 * This file is part of the broadway/read-model-elasticsearch package.
 *
 * (c) 2020 Broadway project
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Broadway\ReadModel\ElasticSearch;

class AliasingElasticSearchRepository extends ElasticSearchRepository
{
    public function createIndexWithAlias(string $suffix): bool
    {
        $indexParams = [
            'index' => $this->index . $suffix,
            'body'  => [
                'aliases' => [
                    $this->index => new \stdClass(),
                ],
            ],
        ];

        if (count($this->notAnalyzedFields)) {
            $indexParams['body'] = [
                'mappings' => [
                    '_source' => [
                        'enabled' => true,
                    ],
                    'properties' => $this->createNotAnalyzedFieldsMapping($this->notAnalyzedFields),
                ],
            ];
        }

        $this->client->indices()->create($indexParams);
        $response = $this->client->cluster()->health([
            'index' => $this->index,
            'wait_for_status' => 'yellow',
            'timeout' => '5s',
        ]);

        return isset($response['status']) && 'red' !== $response['status'];
    }


    /**
     * Switches the alias to the current index, and removes the old indices.
     *
     * @return void
     */
    public function switchToNewIndex()
    {
        $oldIndices = [];
        // If the alias does not exist but there is an index with the same name as the alias, remove it.
        if (!$this->client->indices()->existsAlias(['name' => $this->alias])) {
            if ($this->client->indices()->exists(['index' => $this->alias])) {
                $this->client->indices()->delete(['index' => $this->alias]);
            }
        } else {
            // Find out where the alias points to.
            $oldIndices = array_keys($this->client->indices()->getAlias(['name' => $this->alias]));
        }

        $this->switchAliasToCurrentIndex($oldIndices);
        $this->removeOldIndices($oldIndices);
    }

    private function switchAliasToCurrentIndex(array $oldIndices)
    {
        $batchActions = [];

        foreach ($oldIndices as $oldIndex) {
            $batchActions[] = [
                'remove' => [
                    'index' => $oldIndex,
                    'alias' => $this->alias,
                ],
            ];
        }

        $batchActions[] = [
            'add' => [
                'index' => $this->index,
                'alias' => $this->alias,
            ],
        ];

        $this->client->indices()->updateAliases(['body' => ['actions' => $batchActions]]);
    }

    private function removeOldIndices(array $oldIndices)
    {
        if (count($oldIndices) > 0) {
            $this->client->indices()->delete(['index' => implode(',', $oldIndices)]);
        }
    }
}
