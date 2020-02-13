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

    protected function createElasticSearchRepository(Client $client, Serializer $serializer, string $index, string $class)
    {
        return new ElasticSearchRepository($client, $serializer, $index, $class);
    }

    /**
     * @test
     */
    public function it_creates_an_index_with_non_analyzed_terms()
    {
        $type             = 'class';
        $nonAnalyzedTerm  = 'name';
        $index            = 'test_non_analyzed_index';
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

        $this->assertArrayHasKey($index, $mapping);
        $this->assertArrayHasKey($type, $mapping[$index]['mappings']);
        $nonAnalyzedTerms = [];

        foreach ($mapping[$index]['mappings'][$type]['properties'] as $key => $value) {
            $nonAnalyzedTerms[] = $key;
        }

        $this->assertEquals([$nonAnalyzedTerm], $nonAnalyzedTerms);
    }

    /**
     * @test
     */
    public function it_throws_when_saving_a_readmodel_of_other_type_than_configured()
    {
        $this->expectException('Assert\InvalidArgumentException');
        $readModel = $this->prophesize('\Broadway\ReadModel\Identifiable');

        $this->repository->save($readModel->reveal());
    }

    /**
     * @test
     * {@inheritDoc}
     */
    public function it_returns_all_read_models()
    {
        $model1 = $this->createReadModel('1', 'othillo', 'bar');
        $model2 = $this->createReadModel('2', 'asm89', 'baz');
        $model3 = $this->createReadModel('3', 'edelprino', 'baz');

        $this->repository->save($model1);
        $this->repository->save($model2);
        $this->repository->save($model3);

        $allReadModels = $this->repository->findAll();
        $this->assertTrue(in_array($model1, $allReadModels));
        $this->assertTrue(in_array($model2, $allReadModels));
        $this->assertTrue(in_array($model3, $allReadModels));
    }

    public function tearDown(): void
    {
        $this->client->indices()->delete(['index' => 'test_index']);

        if ($this->client->indices()->exists(['index' => 'test_non_analyzed_index'])) {
            $this->client->indices()->delete(['index' => 'test_non_analyzed_index']);
        }
    }

    private function createClient()
    {
        return (new ElasticSearchClientFactory())->create(['hosts' => ['localhost:9200']]);
    }
}
