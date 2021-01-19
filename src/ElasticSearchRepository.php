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

use Assert\Assertion;
use Assert\AssertionFailedException;
use Broadway\ReadModel\Identifiable;
use Broadway\ReadModel\Repository;
use Broadway\Serializer\Serializer;
use Elasticsearch\Client;
use Elasticsearch\Common\Exceptions\Missing404Exception;
use InvalidArgumentException;
use stdClass;

/**
 * Repository implementation using Elasticsearch as storage.
 */
class ElasticSearchRepository implements Repository
{
    /** @var Client */
    private $client;

    /** @var Serializer */
    private $serializer;

    /** @var string */
    private $index;

    /** @var string */
    private $class_type;

    /** @var string[] */
    private $notAnalyzedFields;

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
     * @throws AssertionFailedException
     */
    public function save(Identifiable $data): void
    {
        if (!class_exists($this->class_type)) {
            throw new InvalidArgumentException(
                "The class type provided ({$this->class_type}) does not exists."
            );
        }

        Assertion::isInstanceOf(
            $data,
            $this->class_type,
            "Data object should be of type {$this->class_type}, as declared on the repository definition."
        );

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

    private function searchAndDeserializeHits(array $query): array
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

    private function buildFindByQuery(array $fields): array
    {
        return [
            'bool' => [
                'must' => $this->buildFilter($fields),
            ],
        ];
    }

    private function buildFindAllQuery(): array
    {
        return [
            'match_all' => new stdClass(),
        ];
    }

    private function deserializeHit(array $hit): Identifiable
    {
        return $this->serializer->deserialize(
            [
                'class' => $this->class_type,
                'payload' => $hit['_source'],
            ]
        );
    }

    private function deserializeHits(array $hits): array
    {
        return array_map([$this, 'deserializeHit'], $hits);
    }

    private function buildFilter(array $filter): array
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

    private function createNotAnalyzedFieldsMapping(array $notAnalyzedFields): array
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
