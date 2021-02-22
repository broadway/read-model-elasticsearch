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

use Broadway\ReadModel\ElasticSearch\Exception\MultiTypeIndexNotAllowedException;
use Broadway\ReadModel\Identifiable;
use Broadway\ReadModel\Repository;
use Broadway\Serializer\Serializer;
use Elasticsearch\Client;
use Elasticsearch\Common\Exceptions\Missing404Exception;
use stdClass;

/**
 * Repository implementation using Elasticsearch as storage.
 */
class ElasticSearchRepository implements Repository
{
    /** @var Client */
    protected $client;

    /** @var Serializer */
    protected $serializer;

    /** @var string */
    protected $index;

    /** @var string */
    protected $class_type;

    /** @var string[] */
    protected $notAnalyzedFields;

    public function __construct(
        Client $client,
        Serializer $serializer,
        string $index,
        string $class_type,
        array $notAnalyzedFields = []
    ) {
        $this->client = $client;
        $this->serializer = $serializer;
        $this->index = $index;
        $this->class_type = $class_type;
        $this->notAnalyzedFields = $notAnalyzedFields;
    }

    /**
     * @param Identifiable $data
     * @throws MultiTypeIndexNotAllowedException
     */
    public function save(Identifiable $data): void
    {
        if (!$data instanceof $this->class_type) {
            throw new MultiTypeIndexNotAllowedException(
                "Data object should be of type {$this->class_type}, as declared on the repository definition."
            );
        }

        $serializedReadModel = $this->serializer->serialize($data);
        $this->client->index([
            'index' => $this->index,
            'id' => $data->getId(),
            'body' => $serializedReadModel['payload'],
            'refresh' => true,
        ]);
    }

    /**
     * {@inheritdoc}
     */
    public function find($id): ?Identifiable
    {
        $params = [
            'index' => $this->index,
            'id' => (string) $id,
        ];

        try {
            $result = $this->client->get($params);
        } catch (Missing404Exception $e) {
            return null;
        }

        return $this->deserializeHit($result);
    }

    /**
     * {@inheritdoc}
     */
    public function findBy(array $fields): array
    {
        if (empty($fields)) {
            return [];
        }

        return $this->query($this->buildFindByQuery($fields));
    }

    /**
     * {@inheritdoc}
     */
    public function findAll(): array
    {
        return $this->query($this->buildFindAllQuery());
    }

    /**
     * {@inheritdoc}
     */
    public function remove($id): void
    {
        try {
            $this->client->delete([
                'id' => (string) $id,
                'index' => $this->index,
                'refresh' => true,
            ]);
        } catch (Missing404Exception $e) {
            // It was already deleted or never existed, fine by us!
        }
    }

    protected function searchAndDeserializeHits(array $query): array
    {
        try {
            $result = $this->client->search($query);
        } catch (Missing404Exception $e) {
            return [];
        }

        if (!array_key_exists('hits', $result)) {
            return [];
        }

        return $this->deserializeHits($result['hits']['hits']);
    }

    protected function search(array $query, array $facets = [], int $size = 500): array
    {
        try {
            return $this->client->search([
                'index' => $this->index,
                'body' => [
                    'query' => $query,
                    'facets' => $facets,
                ],
                'size' => $size,
            ]);
        } catch (Missing404Exception $e) {
            return [];
        }
    }

    protected function query(array $query): array
    {
        return $this->searchAndDeserializeHits([
            'index' => $this->index,
            'body' => [
                'query' => $query,
            ],
            'size' => 500,
        ]);
    }

    protected function buildFindByQuery(array $fields): array
    {
        return [
            'bool' => [
                'must' => $this->buildFilter($fields),
            ],
        ];
    }

    protected function buildFindAllQuery(): array
    {
        return [
            'match_all' => new stdClass(),
        ];
    }

    protected function deserializeHit(array $hit): Identifiable
    {
        return $this->serializer->deserialize(
            [
                'class' => $this->class_type,
                'payload' => $hit['_source'],
            ]
        );
    }

    protected function deserializeHits(array $hits): array
    {
        return array_map([$this, 'deserializeHit'], $hits);
    }

    protected function buildFilter(array $filter): array
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
     * @return bool True, if the index was successfully created
     */
    public function createIndex(): bool
    {
        $indexParams = [
            'index' => $this->index,
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
     * Deletes the index for this repository's ReadModel.
     */
    public function deleteIndex(): bool
    {
        $indexParams = [
            'index' => $this->index,
            'timeout' => '5s',
        ];

        $this->client->indices()->delete($indexParams);

        $response = $this->client->cluster()->health([
            'index' => $this->index,
            'wait_for_status' => 'yellow',
            'timeout' => '5s',
        ]);

        return isset($response['status']) && 'red' !== $response['status'];
    }

    protected function createNotAnalyzedFieldsMapping(array $notAnalyzedFields): array
    {
        $fields = [];

        foreach ($notAnalyzedFields as $field) {
            $fields[$field] = [
                'type' => 'keyword',
            ];
        }

        return $fields;
    }
}
