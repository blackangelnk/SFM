<?php
namespace SFM\IdentityMap;

use SFM\Entity;
use SFM\Transaction\TransactionEngineInterface;
use SFM\Transaction\TransactionException;

/**
 * Identity Map for already registered objects
 */
class IdentityMap implements IdentityMapInterface, TransactionEngineInterface
{
    protected $isEnabled = true;

    /**
     * @var int
     */
    protected $transactionDepth = 0;

    /** @var IdentityMapStorageInterface  */
    protected $storage;

    /** @var IdentityMapStorageInterface */
    protected $transactionAddStorage;

    /** @var IdentityMapStorageInterface */
    protected $transactionRemoveStorage;

    /** @var IdentityMapStorageInterface[] */
    protected $transactionAddStoragesContainer;

    /** @var IdentityMapStorageInterface[] */
    protected $transactionRemoveStoragesContainer;

    /**
     * @param IdentityMapStorageInterface $storage
     * @param IdentityMapStorageInterface $storageTransactionAdd
     * @param IdentityMapStorageInterface $storageTransactionRemove
     */
    public function __construct(IdentityMapStorageInterface $storage, IdentityMapStorageInterface $storageTransactionAdd,
                                IdentityMapStorageInterface $storageTransactionRemove)
    {
        $this->storage = $storage;
        $this->transactionAddStorage = $storageTransactionAdd;
        $this->transactionRemoveStorage = $storageTransactionRemove;
    }

    /**
     * Add entity to identity map
     *
     * @param Entity $entity
     * @return IdentityMapInterface
     */
    public function addEntity(Entity $entity)
    {
        if ($this->isEnabled) {

            if ($this->isTransaction()) {
                $this->transactionAddStoragesContainer[$this->transactionDepth - 1]->put($entity);
                $this->transactionRemoveStoragesContainer[$this->transactionDepth - 1]->remove(get_class($entity), $entity->getId());
            } else {
                $this->storage->put($entity);
            }
        }

        return $this;
    }

    /**
     * Get entity from identity map
     *
     * @param string $className
     * @param int $id
     * @return null|Entity
     */
    public function getEntity($className, $id)
    {
        if (!$this->isEnabled) {
            return null;
        }

        $entity = null;
        if ($this->isTransaction()) {
            for ($depth = $this->transactionDepth; $depth > 0; $depth--) {
                $entity = $this->transactionAddStoragesContainer[$depth - 1]->get($className, $id);
                if ($this->transactionRemoveStoragesContainer[$depth - 1]->get($className, $id)) {
                    return null;
                } elseif ($entity) {
                    return $entity;
                }
            }
        }

        if (!$entity instanceof Entity) {
            $entity = $this->storage->get($className, $id);
        }

        return $entity;
    }

    /**
     * Get multiple entities from identity map
     *
     * @param string $className
     * @param \int[] $ids
     * @return \SFM\Entity[]
     */
    public function getEntityMulti($className, $ids)
    {
        if (!$this->isEnabled) {
            return [];
        }

        $entities = [];
        $keysFromCache = array_merge($ids);
        if ($this->isTransaction()) {
            for ($depth = $this->transactionDepth; $depth > 0; $depth--) {
                $transactionLevelEntities = $this->transactionAddStoragesContainer[$depth - 1]->getM($className, $ids);
                foreach ($this->transactionRemoveStoragesContainer[$depth - 1]->getM($className, $ids) as $entity) {
                    unset($transactionLevelEntities[$entity->getId()]);
                }
                $keysFromCache = array_diff($keysFromCache, array_keys($transactionLevelEntities));
                $entities[] = $transactionLevelEntities;
            }
        }

        $entities = array_merge($this->storage->getM($className, $keysFromCache), $entities);

        return $entities;
    }

    /**
     * Delete entity from identity map
     *
     * @param Entity $entity
     * @return IdentityMapInterface
     */
    public function deleteEntity(Entity $entity)
    {
        if ($this->isEnabled) {

            if ($this->isTransaction()) {
                $this->transactionRemoveStoragesContainer[$this->transactionDepth - 1]->put($entity);
                $this->transactionAddStoragesContainer[$this->transactionDepth - 1]->remove(get_class($entity), $entity->getId());
            } else {
                $this->storage->remove(get_class($entity), $entity->getId());
            }
        }

        return $this;
    }

    /**
     * Enable identity map
     *
     * @return IdentityMapInterface
     */
    public function enable()
    {
        $this->isEnabled = true;

        return $this;
    }

    /**
     * Disable identity map
     *
     * @return IdentityMapInterface
     */
    public function disable()
    {
        $this->isEnabled = false;

        return $this;
    }

    /**
     * @throws \SFM\Transaction\TransactionException
     */
    public function beginTransaction()
    {
        $this->transactionAddStoragesContainer[] = clone $this->transactionAddStorage;
        $this->transactionRemoveStoragesContainer[] = clone $this->transactionRemoveStorage;

        $this->transactionDepth++;
    }

    /**
     * @throws \SFM\Transaction\TransactionException
     */
    public function commitTransaction()
    {
        if (!$this->isTransaction()) {
            throw new TransactionException('Transaction already stopped');
        }
        $transactionRemoveStorage = array_pop($this->transactionRemoveStoragesContainer);
        $transactionAddStorage = array_pop($this->transactionAddStoragesContainer);
        $this->transactionDepth--;

        /** @var string $className */
        foreach ($transactionRemoveStorage->getClassNames() as $className) {
            /** @var Entity $entity */
            foreach ($transactionRemoveStorage->getM($className) as $entity) {
                if ($this->transactionDepth === 0) {
                    $this->storage->remove($className, $entity->getId());
                } else {
                    $this->transactionAddStoragesContainer[$this->transactionDepth - 1]->remove($className, $entity->getId());
                }
            }
        }

        /** @var string $className */
        foreach ($transactionAddStorage->getClassNames() as $className) {
            /** @var Entity $entity */
            foreach ($transactionAddStorage->getM($className) as $entity) {
                if ($this->transactionDepth === 0) {
                    $this->storage->put($entity);
                } else {
                    $this->transactionAddStoragesContainer[$this->transactionDepth - 1]->put($entity);
                }
            }
        }
    }

    /**
     * @throws \SFM\Transaction\TransactionException
     */
    public function rollbackTransaction()
    {
        if (!$this->isTransaction()) {
            throw new TransactionException('Transaction already stopped');
        }
        $lastTransactionRemoveStorage = array_pop($this->transactionRemoveStoragesContainer);
        $lastTransactionAddStorage = array_pop($this->transactionAddStoragesContainer);
        $this->transactionDepth--;

	// remove all data that was changed during transaction from storage
        $changedData = array_merge(
            $lastTransactionAddStorage->getClassNames(),
            $lastTransactionRemoveStorage->getClassNames()
        );
        /** @var string $className */
        foreach ($changedData as $className) {
            foreach ($this->transactionRemoveStoragesContainer as $key => $transactionRemoveStorage) {
                /** @var Entity $entity */
                foreach ($transactionRemoveStorage->getM($className) as $entity) {
                    $this->storage->remove($className, $entity->getId());
                    $this->transactionRemoveStoragesContainer[$key]->remove($className, $entity->getId());
                }
            }
            foreach ($this->transactionAddStoragesContainer as $key => $transactionAddStorage) {
                /** @var Entity $entity */
                foreach ($transactionAddStorage->getM($className) as $entity) {
                    $this->storage->remove($className, $entity->getId());
                    $this->transactionAddStoragesContainer[$key]->remove($className, $entity->getId());
                }
            }
        }
    }

    /**
     * @return bool
     */
    public function isTransaction()
    {
        return $this->transactionDepth > 0;
    }
}
