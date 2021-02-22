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
use Broadway\ReadModel\ElasticSearch\Utils\AnotherRepositoryTestReadModel;
use Broadway\ReadModel\Repository;
use Broadway\ReadModel\Testing\RepositoryTestCase;
use Broadway\ReadModel\Testing\RepositoryTestReadModel;
use Broadway\Serializer\Serializer;
use Broadway\Serializer\SimpleInterfaceSerializer;
use Elasticsearch\Client;

/**
 * @group functional
 * @requires extension curl
 */
class ElasticSearchRepositoryTest extends RepositoryTestCase
{
    private $client;

    protected function createRepository(): Repository
    {
        $this->client = $this->createClient();
        $this->client->indices()->create(['index' => 'test_index']);
        $this->client->cluster()->health(['index' => 'test_index', 'wait_for_status' => 'yellow', 'timeout' => '10s']);

        return $this->createElasticSearchRepository(
            $this->client,
            new SimpleInterfaceSerializer(),
            'test_index',
            RepositoryTestReadModel::class
        );
    }

    protected function createElasticSearchRepository(
        Client $client,
        Serializer $serializer,
        string $index,
        string $class_type
    ): ElasticSearchRepository {
        return new ElasticSearchRepository($client, $serializer, $index, $class_type);
    }

    /**
     * @test
     */
    public function it_creates_an_index_with_non_analyzed_terms(): void
    {
        $type = 'class';
        $nonAnalyzedTerm = 'name';
        $index = 'test_non_analyzed_index';
        $this->repository = new ElasticSearchRepository(
            $this->client,
            new SimpleInterfaceSerializer(),
            $index,
            $type,
            [$nonAnalyzedTerm]
        );

        $this->repository->createIndex();
        $this->client->cluster()->health(['index' => $index, 'wait_for_status' => 'yellow', 'timeout' => '10s']);
        $mapping = $this->client->indices()->getMapping(['index' => $index]);

        $expectedMapping = [
            'test_non_analyzed_index' => [
                'mappings' => [
                    'properties' => [
                        'name' => [
                            'type' => 'keyword',
                        ],
                    ],
                ],
            ],
        ];
        self::assertEquals($expectedMapping, $mapping);
    }

    /**
     * @test
     */
    public function it_throws_when_trying_to_save_different_class_types_in_the_same_repository(): void
    {
        $this->expectException(MultiTypeIndexNotAllowedException::class);
        $this->expectExceptionMessage('Data object should be of type');

        $model1 = $this->createReadModel('1', 'othillo', 'bar');
        $this->repository->save($model1);

        $model2 = new AnotherRepositoryTestReadModel('2', 'someoneelse');
        $this->repository->save($model2);
    }

    /**
     * @test
     */
    public function it_returns_all_read_models(): void
    {
        $model1 = $this->createReadModel('1', 'othillo', 'bar');
        $model2 = $this->createReadModel('2', 'asm89', 'baz');
        $model3 = $this->createReadModel('3', 'edelprino', 'baz');

        $this->repository->save($model1);
        $this->repository->save($model2);
        $this->repository->save($model3);

        $allReadModels = $this->repository->findAll();
        self::assertContainsEquals($model1, $allReadModels);
        self::assertContainsEquals($model2, $allReadModels);
        self::assertContainsEquals($model3, $allReadModels);
    }

    /**
     * @test
     */
    public function it_creates_an_index_with_an_alias()
    {
        $type             = 'class';
        $nonAnalyzedTerm  = 'name';
        $alias            = 'test_non_analyzed_index';
        $this->repository = new ElasticSearchRepository(
            $this->client,
            new SimpleInterfaceSerializer(),
            $alias,
            $type,
            array($nonAnalyzedTerm)
        );

        $suffix = uniqid('', false);
        $index  = $alias . $suffix;

        $this->repository->createIndex($suffix);
        $this->client->cluster()->health(array('index' => $index, 'wait_for_status' => 'yellow', 'timeout' => '10s'));

        $expectedAlias = [
            $index => [
                'aliases' => [
                    $alias => []
                ]
            ]
        ];

        $this->assertEquals($expectedAlias, $this->client->indices()->getAlias(['name' => $alias]));
    }

    public function tearDown(): void
    {
        $this->client->indices()->delete(['index' => 'test_index']);

        if ($this->client->indices()->exists(['index' => 'test_non_analyzed_index'])) {
            $this->client->indices()->delete(['index' => 'test_non_analyzed_index']);
        }
    }

    protected function createClient(): Client
    {
        return (new ElasticSearchClientFactory())->create(['hosts' => ['localhost:9200']]);
    }
}
