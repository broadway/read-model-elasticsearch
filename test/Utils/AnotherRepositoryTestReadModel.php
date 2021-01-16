<?php

declare(strict_types=1);

namespace Broadway\ReadModel\ElasticSearch\Utils;

use Broadway\ReadModel\SerializableReadModel;

class AnotherRepositoryTestReadModel implements SerializableReadModel
{
    private $id;
    private $name;

    /**
     * @param mixed $id
     * @param mixed $name
     */
    public function __construct($id, string $name)
    {
        $this->id = (string) $id;
        $this->name = $name;
    }

    public function getId(): string
    {
        return $this->id;
    }

    public function getName(): string
    {
        return $this->name;
    }

    public function serialize(): array
    {
        return get_object_vars($this);
    }

    public static function deserialize(array $data): AnotherRepositoryTestReadModel
    {
        return new self($data['id'], $data['name']);
    }
}
