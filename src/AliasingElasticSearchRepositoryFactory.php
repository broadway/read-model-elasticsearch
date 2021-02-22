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

use Broadway\ReadModel\Repository;

class AliasingElasticSearchRepositoryFactory extends ElasticSearchRepositoryFactory
{
    /**
     * {@inheritDoc}
     */
    public function create($name, $class, array $notAnalyzedFields = []): Repository
    {
        return new AliasingElasticSearchRepository($this->client, $this->serializer, $name, $class, $notAnalyzedFields);
    }
}
