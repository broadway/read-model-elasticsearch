<?php

/*
 * This file is part of the broadway/read-model-elasticsearch package.
 *
 * (c) Qandidate.com <opensource@qandidate.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Broadway\ReadModel\ElasticSearch;

use Assert\Assertion;
use Broadway\ReadModel\Identifiable;
use Broadway\ReadModel\Repository;
use Broadway\Serializer\Serializer;
use Elasticsearch\Client;
use Elasticsearch\Common\Exceptions\Missing404Exception;

/**
 * Repository implementation using Elasticsearch as storage.
 */
class ElasticSearchRepository implements Repository
{
    private $client;
    private $serializer;
    private $index;
    private $class;
    private $notAnalyzedFields;

    public function __construct(
        Client $client,
        Serializer $serializer,
        string $index,
        string $class,
        array $notAnalyzedFields = []
    ) {
        $this->client            = $client;
        $this->serializer        = $serializer;
        $this->index             = $index;
        $this->class             = $class;
        $this->notAnalyzedFields = $notAnalyzedFields;
    }

    /**
     * {@inheritDoc}
     */
    public function save(Identifiable $data)
    {
        Assertion::isInstanceOf($data, $this->class);

        $serializedReadModel = $this->serializer->serialize($data);

        $params = [
            'index'   => $this->index,
            'type'    => $serializedReadModel['class'],
            'id'      => $data->getId(),
            'body'    => $serializedReadModel['payload'],
            'refresh' => true,
        ];

        $this->client->index($params);
    }

    /**
     * {@inheritDoc}
     */
    public function find($id)
    {
        $params = [
            'index' => $this->index,
            'type'  => $this->class,
            'id'    => $id,
        ];

        try {
            $result = $this->client->get($params);
        } catch (Missing404Exception $e) {
            return null;
        }

        return $this->deserializeHit($result);
    }

    /**
     * {@inheritDoc}
     */
    public function findBy(array $fields): array
    {
        if (empty($fields)) {
            return [];
        }

        return $this->query($this->buildFindByQuery($fields));
    }

    /**
     * {@inheritDoc}
     */
    public function findAll(): array
    {
        return $this->query($this->buildFindAllQuery());
    }

    /**
     * {@inheritDoc}
     */
    public function remove($id)
    {
        try {
            $this->client->delete([
                'id'      => $id,
                'index'   => $this->index,
                'type'    => $this->class,
                'refresh' => true,
            ]);
        } catch (Missing404Exception $e) { // It was already deleted or never existed, fine by us!
        }
    }

    private function searchAndDeserializeHits(array $query)
    {
        try {
            $result = $this->client->search($query);
        } catch (Missing404Exception $e) {
            return [];
        }

        if (! array_key_exists('hits', $result)) {
            return [];
        }

        return $this->deserializeHits($result['hits']['hits']);
    }

    protected function search(array $query, array $facets = [], int $size = 500): array
    {
        try {
            return $this->client->search([
                'index' => $this->index,
                'type'  => $this->class,
                'body'  => [
                    'query'  => $query,
                    'facets' => $facets,
                ],
                'size' => $size,
            ]);
        } catch (Missing404Exception $e) {
            return [];
        }
    }

    protected function query(array $query)
    {
        return $this->searchAndDeserializeHits(
            [
                'index' => $this->index,
                'type'  => $this->class,
                'body'  => [
                    'query' => $query,
                ],
                'size'  => 500,
            ]
        );
    }

    private function buildFindByQuery(array $fields): array
    {
        return [
            'bool' => [
                'must' => $this->buildFilter($fields)
            ]
        ];
    }

    private function buildFindAllQuery(): array
    {
        return [
            'match_all' => new \stdClass(),
        ];
    }

    private function deserializeHit(array $hit)
    {
        return $this->serializer->deserialize(
            [
                'class'   => $hit['_type'],
                'payload' => $hit['_source'],
            ]
        );
    }

    private function deserializeHits(array $hits)
    {
        return array_map([$this, 'deserializeHit'], $hits);
    }

    private function buildFilter(array $filter)
    {
        $retval = [];

        foreach ($filter as $field => $value) {
            $retval[] = ['term' => [$field => $value]];
        }

        return $retval;
    }

    /**
     * Creates the index for this repository's ReadModel.
     *
     * @return boolean True, if the index was successfully created
     */
    public function createIndex(): bool
    {
        $class = $this->class;

        $indexParams = [
            'index' => $this->index,
        ];

        if (count($this->notAnalyzedFields)) {
            $indexParams['body'] = [
                'mappings' => [
                    $class => [
                        '_source'    => [
                            'enabled' => true
                        ],
                        'properties' => $this->createNotAnalyzedFieldsMapping($this->notAnalyzedFields),
                    ]
                ]
            ];
        }

        $this->client->indices()->create($indexParams);
        $response = $this->client->cluster()->health([
            'index'           => $this->index,
            'wait_for_status' => 'yellow',
            'timeout'         => '5s',
        ]);

        return isset($response['status']) && $response['status'] !== 'red';
    }

    /**
     * Deletes the index for this repository's ReadModel.
     *
     * @return True, if the index was successfully deleted
     */
    public function deleteIndex(): bool
    {
        $indexParams = [
            'index'   => $this->index,
            'timeout' => '5s',
        ];

        $this->client->indices()->delete($indexParams);

        $response = $this->client->cluster()->health([
            'index'           => $this->index,
            'wait_for_status' => 'yellow',
            'timeout'         => '5s',
        ]);

        return isset($response['status']) && $response['status'] !== 'red';
    }

    private function createNotAnalyzedFieldsMapping(array $notAnalyzedFields): array
    {
        $fields = [];

        foreach ($notAnalyzedFields as $field) {
            $fields[$field] = [
                'type'  => 'string',
                'index' => 'not_analyzed'
            ];
        }

        return $fields;
    }
}
