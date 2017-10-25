<?php
namespace SFM;

use SFM\Cache\CacheProvider;
use Zend\Db\Adapter\Driver\Pgsql\Pgsql;

/**
 * Abstract class for Data Mapping
 */
abstract class Mapper
{

    /**
     * Table that contains data of Business objects
     * Notice: for the time present we assume that it is single table
     * @var string
     */
    protected $tableName;

    /**
     * Name of id field in DB
     *
     * @var string
     */
    protected $idField;

    /**
     * List of fields unique corresponds to id field
     * Useful to get entity id quickly.
     * @example andry.domain.com => domain.com/?user_id=1 and domain.com/search?test@domain.com => domain.com/search?user_id=1 and zipcode&country
     * array(array(login), array(email), array(zipcode, country))
     *
     * @var array[array]
     */
    protected $uniqueFields = array();

    /**
     * Prefix for aggregates
     *
     * @var string
     */
    protected $aggregateCachePrefix;

    /**
     * Name of entity class that linked with mapper
     * @var string
     */
    protected $entityClassName;
    /**
     * Name of aggregate class that linked with mapper
     * @var string
     */
    protected $aggregateClassName;

    const SQL_PARAM_LIMIT = '_LIMIT_';
    const SQL_PARAM_ORDER_BY = '_ORDER_BY_';
    const SQL_FIELD = '_field_';
    const SQL_PARAM_ORDER_SORT = '_sort_';
    const SQL_PARAM_ORDER_ASC = 'ASC';
    const SQL_PARAM_ORDER_DESC = 'DESC';
    const SQL_PARAM_GROUP_BY = '_GROUP_BY_';
    const SQL_SELECT_TYPE = '_select_?_from_';
    const SQL_SELECT_ALL = '_select_*_from_';
    const SQL_SELECT_ID = '_select_id_from_';
    const SQL_PARAM_CONDITION = '_CONDITION_';

    /** @var Repository */
    protected $repository;

    /**
     * @return Repository
     */
    public function getRepository()
    {
        return $this->repository;
    }

    /**
     * @param Repository $repository
     */
    public function __construct(Repository $repository)
    {
        $this->repository = $repository;

        $className = get_class($this);
        $this->entityClassName = str_replace('Mapper', 'Entity', $className);
        $this->aggregateClassName = str_replace('Mapper', 'Aggregate', $className);
        $this->idField = 'id';
        $this->aggregateCachePrefix = $this->aggregateClassName . CacheProvider::KEY_DELIMITER;
    }

    public function getTableName()
    {
        return $this->tableName;
    }

    /**
     * Returns name of field that is unique to every Entity.
     * Entity has no ability to change it
     *
     * @return string
     */
    public function getIdField()
    {
        return $this->idField;
    }

    /**
     * Returns list of fields that is unique to every Entity.
     * Entity has no ability to change it
     *
     * @return array
     */
    public function getUniqueFields()
    {
        return $this->uniqueFields;
    }



    /**
     * Get Entity by id.
     * First tries to fetch Entity from cache, than looks to DB and caches fetched Entity
     * If Entity can't be fetched null is returned
     *
     * @param int $id
     * @return Entity|null
     * @throws BaseException
     */
    public function getEntityById( $id )
    {
        if (is_numeric($id) && $id > 0) {
            //First looks to IndentityMap
            $entity = $this->getEntityFromIdentityMap($this->entityClassName, $id);
            if( null !== $entity ) {
                return $entity;
            }
            //Second looks to Cache
            $cacheKey = $this->getEntityCacheKeyById($id);
            $entity = Manager::getInstance()->getCache()->get($cacheKey);
//          //aazon: check either Entity is Cacheable. We need this hack till we refactor updateEntity()
            if (null !== $entity && $entity->isCacheable()) {
                return $entity;
            }
        } else {
            throw new BaseException("Illegal argument type given; id: ".$id);
        }
        //Then looks to DB. Check that Entity exists
        $entity = $this->getEntityFromDB(array($this->getIdField()=>$id));

        if( null !== $entity && $entity->isCacheable() ) {
            //Store Entity in Cache. Since now we store only cacheable entities
            $this->saveCached($entity);
        }
        return $entity;

    }

    /**
     * Get Entity by unique fields.
     * Note: You can use only two ways to guarantee single object is received:
     *  - getEntityById
     *  - getEntityByUniqueFields
     * @see getEntityById
     * @param $params
     * @return Entity|null
     * @throws BaseException
     */
    public function getEntityByUniqueFields(array $params)
    {
        if ( $this->hasUniqueFields() && ($params = $this->getOneUniqueFromParams($params)) ) {
            $cacheKey = $this->getEntityCacheKeyByUniqueVals( $this->getUniqueVals($params) );
            $entityId = Manager::getInstance()->getCache()->getRaw($cacheKey);
            if( null !== $entityId ) {
                return $this->getEntityById( $entityId );
            }
        } else {
            throw new BaseException("Unique fields aren't set");
        }

        $entity = $this->getEntityFromDB($params);

        //aazon: now we check either Entity is cacheable
        if( null !== $entity && $entity->isCacheable() ) {
            //to prevent unique fields mapping to empty cache object
            if( null === Manager::getInstance()->getCache()->get($entity->getCacheKey())) {
                $this->saveCached($entity);
            }
            $uniqueKey = array_keys($params);
            $this->createUniqueFieldsCache($entity, $uniqueKey);
        }

        return $entity;
    }


    /**
     * Wrapper. Thank the method's author for spelling mistakes!
     * @param array $params
     * @deprecated
     */
    public function getEntityByUniqueFileds(array $params)
    {
        return $this->getEntityByUniqueFields($params);
    }


    /**
     * Returns key for storing entity in cache.
     * Actually, this method should be called from Entity, but Entity can't know its idField,
     * thats why Entity method calls Mapper method to fetch key. Tre relationship between Entitty and Mapper
     * is something like friendship.
     * Since Mapper must has ability to fetch Cache key before creation Entity, we have to invent
     * protected method Mapper::getEntityCacheKeyById()
     *
     * @param Entity $entity
     * @return string
     */
    public function getEntityCacheKey(Entity $entity)
    {
        return $this->getEntityCacheKeyById($entity->getInfo($this->idField));
    }
    /**
     * @param Entity $entity
     * @param array $uniqueKey One of the keys. It must contain only field names
     * @return string
     * @throws BaseException
     */
    public function getEntityCacheKeyByUniqueFields(Entity $entity, array $uniqueKey)
    {
        $uniqueVals = array();

        foreach ($uniqueKey as $uniqueKeyItem) {
            if(!is_array($uniqueKeyItem))
                $uniqueKeyItem = array($uniqueKeyItem);
            foreach ($uniqueKeyItem as $item) {
                $val = $entity->getInfo($item);
                if( null !== $val ) {
                    if(is_string($val)) {
                        $val = mb_strtolower($val);
                    }
                    $uniqueVals[] = $val;
                } else {
                    throw new BaseException('Unknown field - '.$item);
                }
            }
        }
        return $this->getEntityCacheKeyByUniqueVals( $uniqueVals );
    }


    /**
     * Returns Entity object by prototype $proto
     * @param array $proto
     * @return Entity must be overrided in children
     */
    public function createEntity(array $proto)
    {
        $className = $this->entityClassName;
        if(array_key_exists($this->idField, $proto)) {
            $entity = $this->getEntityFromIdentityMap($className, $proto[$this->idField]);
        } else {
            $entity = null;
        }
        if ($entity === null) {
            $entity = new $className($proto, $this);
            Manager::getInstance()->getIdentityMap()->addEntity($entity);
        }
        return $entity;
    }

    /**
     * Return appropriate prepared statement
     *
     * @param $keyName
     * @param $keyCounter
     * @return string
     */
    protected function getPreparedParam($keyName, $keyCounter)
    {
        $driver = Manager::getInstance()->getDb()->getAdapter()->getDriver();
        if ($driver instanceof Pgsql) {
            return '$' . $keyCounter;
        }

        return ':' . $keyName;
    }

    /**
     * Updates Entity in Database
     * Do not call this method directly! Use Entity::update
     * @todo Check values from $params to be in datamap
     *
     * @param array $params
     * @param Entity $entity
     * @return bool
     */
    public function updateEntity(array $params, Entity $entity)
    {
        //@TODO rewrite without clone
        if (empty($params)) {
            return true;
        }

        $oldEntity = clone $entity;

        //Prevent changing id of Entity
        unset($params[$this->idField]);

        //First update the DB
        $updates = array();
        $keyCounter = 1;
        foreach ($params as $key => $value) {
            $field = Manager::getInstance()->getDb()->quoteIdentifier($key);
            $updates[]= "{$field}=" . $this->getPreparedParam($key, $keyCounter);
            $keyCounter++;
        }

        $params[$this->idField] = $entity->getInfo($this->idField);
        $sql = "UPDATE ".Manager::getInstance()->getDb()->quoteIdentifier($this->tableName)." SET " . implode(',', $updates) . " WHERE {$this->idField}=" . $this->getPreparedParam($this->idField, $keyCounter);

        $state = Manager::getInstance()->getDb()->update($sql, $params);

        //replace in indentityMap
        Manager::getInstance()->getIdentityMap()->addEntity($entity);
        //Then save to Cache. Tags will be reset automatically
        if ($entity->isCacheable()) {
            $this->saveCached($entity);
        }

        foreach ($params as $key => $value) {
            //Check that field exists
            if (array_key_exists($key, $entity->getProto())) {
                $entity->setProto($key, $value);

                //if it is an some id-field...
                if(strrpos($key,'_id') !== false) {
                    //...and if there is a lazy-object loaded already...
                    if ($entity->getComputed($key) instanceof Entity) {
                        //...kill it. Goodbye!
                        $entity->deleteComputed($key);
                    }
                }
            }
        }
        $this->updateUniqueFields($entity, $oldEntity);

        return $state;
    }

    /**
     * Updates Agregate in Cache
     *
     * @param Aggregate $aggregate
     */
    public function updateAggregate(Aggregate $aggregate)
    {
        $this->saveCached($aggregate);
    }

    /**
     * Search in $newEntity new values of unique fields and update key if needed
     *
     * @param Entity $newEntity
     * @param entity $oldEntity
     * @return void
     */
    public function updateUniqueFields(Entity $newEntity, Entity $oldEntity )
    {
        $changedUniqueKeys = array();

        if($this->hasUniqueFields()) {
            foreach ($this->uniqueFields as $uniqueKey) {
                foreach ( $uniqueKey as $field ) {
                    if( $oldEntity->getInfo($field) != $newEntity->getInfo($field)) {
                        $changedUniqueKeys[] = $uniqueKey;
                    }
                }
            }
            if( sizeof($changedUniqueKeys) != 0 ) {
                foreach ($changedUniqueKeys as $key) {
                    Manager::getInstance()->getCache()->delete($oldEntity->getCacheKeyByUniqueFields($key));
                    $this->createUniqueFieldsCache( $newEntity, $key );
                }
            }
        }
    }

    /**
     * Deletes Entity from Database
     *
     * @param Entity $entity
     * @return bool
     */
    public function deleteEntity(Entity $entity)
    {
        //delete from identity map
        Manager::getInstance()->getIdentityMap()->deleteEntity($entity);
        //delete from Cache
        $Cache = Manager::getInstance()->getCache();
        $Cache->deleteEntity($entity);
        if($this->hasUniqueFields()) {
             foreach ( $this->uniqueFields as $uniqueKey ) {
                 $key = $entity->getCacheKeyByUniqueFields($uniqueKey);
                 $Cache->delete($key);
             }
        }

        //Then delete from DB
        $tableName = Manager::getInstance()->getDb()->quoteIdentifier($this->tableName);

        $sql = "DELETE FROM {$tableName} WHERE {$this->idField}=" . $this->getPreparedParam($this->idField, 1);
        return Manager::getInstance()->getDb()->delete($sql, array($this->idField => $entity->getInfo($this->idField)));
    }

    /**
     * Executes insert SQL query and returns Entity
     *
     * @param $proto
     * @return Entity
     */
    public function insertEntity($proto)
    {
        if($this->isIdAutoIncrement()){
            unset($proto[$this->idField]);
        }

        $keys = array();
        $values = array();
        $keyCounter = 1;
        foreach ($proto as $key => $value) {
            $keys[] = Manager::getInstance()->getDb()->quoteIdentifier($key);
            $values[] = $this->getPreparedParam($key, $keyCounter);
            $keyCounter++;
        }

        $sql = "INSERT INTO "
             . Manager::getInstance()->getDb()->quoteIdentifier($this->tableName)
             . ' (' . implode(', ', $keys) . ') '
             . 'VALUES (' . implode(', ', $values) . ')';

        $id = Manager::getInstance()->getDb()->insert($sql, $proto, $this->idField, $this->isIdAutoIncrement(), $this->tableName);
        return $this->getEntityById($id);
    }

    /**
     * Returns aggregate object by $proto array. Also saves the cache key for Aggregate.
     * The not null value of $cacheKey means that Aggregate in the future will be stored in Cache
     *
     * @param array $proto
     * @param string $cacheKey
     * @return Aggregate must be overrided in childs
     */
    public function createAggregate(array $proto, $cacheKey=null, $loadEntities=false)
    {
        $className = $this->aggregateClassName;
        return new $className($proto, $this, $cacheKey, $loadEntities);
    }

    /**
     * Returns Aggregate by params.
     * By default first looks to Cache, then to DB
     * Since Aggregates have no id, keys for them must be set by developer
     * If $cacheKey is null there will be no look up to Cache
     *
     * @param array $params
     * @param string $cacheKey
     * @param bool $loadEntities
     * @return Aggregate
     */
    public function getAggregate(array $params = array(), $cacheKey=null, $loadEntities=false)
    {
        //If there is a key for Cache, look to Cache
        $aggregate = $this->getCachedAggregate($cacheKey,$loadEntities);
        if($aggregate === null){
            //Look to DB
            $proto = $this->fetchArrayFromDB($params);
            $aggregate = $this->createAggregate($proto, $cacheKey, $loadEntities);
            $this->saveCachedAggregate($aggregate,$loadEntities,0);
        }
        return $aggregate;
    }

    /**
     * The same as getAggregate, but by sql query
     * @see getAggregate
     *
     * @param string $sql
     * @param array $params
     * @param string $cacheKey
     * @param bool $loadEntities
     * @param integer $expiration
     * @return Aggregate
     */
    public function getAggregateBySQL($sql, array $params = array(), $cacheKey=null, $loadEntities=false, $expiration = 0)
    {
        $aggregate = $this->getCachedAggregate($cacheKey,$loadEntities);
        if($aggregate === null){
            $db = Manager::getInstance()->getDb();
            $aggregate = $this->createAggregate( $db->fetchAll($sql, $params), $cacheKey, $loadEntities );
            $this->saveCachedAggregate($aggregate,$loadEntities,$expiration);
        }
        return $aggregate;
    }

    /**
     * The same as getAggregate, but by array of ids
     * @see getAggregate
     *
     * @param array $ids
     * @param string $cacheKey
     * @param bool $loadEntities
     * @param integer $expiration
     * @return Aggregate
     */
    public function getAggregateByIds(array $ids = array(), $cacheKey=null, $loadEntities=false, $expiration = 0)
    {
        $aggregate = $this->getCachedAggregate($cacheKey,$loadEntities);
        if ($aggregate === null) {
            $aggregate = $this->createAggregate( $ids, $cacheKey, $loadEntities );
            $this->saveCachedAggregate($aggregate,$loadEntities,$expiration);
        }
        return $aggregate;
    }

    protected function getCachedAggregate($cacheKey,$loadEntities)
    {
        if ($cacheKey !== null) {
            $aggregate = Manager::getInstance()->getCache()->get($cacheKey);
            if ($aggregate !== null) {
                if( $loadEntities ) {
                    $aggregate->loadEntities();
                }
                return $aggregate;
            }
        }
        return null;
    }

    protected function saveCachedAggregate(Aggregate $aggregate,$loadEntities,$expiration)
    {
        if($expiration){
            $aggregate->setExpires($expiration);
        }
        //If key for Cache exists, store to Caching
        if ($aggregate->getCacheKey() !== null && $aggregate !== null) {
            $this->saveCached($aggregate);
            if( $loadEntities ) {
                $this->saveListOfEntitiesInCache($aggregate->getContent());
            }
        }
    }

    /**
     * Wrapper for getAggregateBySql with load all Entities
     * @see getAggregateBySql
     *
     * @param string $sql
     * @param array $params
     * @param string $cacheKey
     * @param integer $expiration
     * @return Aggregate
     * @throws \Exception
     */
    public function getLoadedAggregateBySQL($sql, array $params = array(), $cacheKey=null, $expiration = 0)
    {
        $tmp = strtolower( str_replace(' ', '', $sql) );
        if( !preg_match('/select([^.]*)(\.{0,1})\*/', $tmp)) {
            throw new \Exception('You must use "SELECT * FROM" to load aggregate');
        }
        return $this->getAggregateBySQL($sql, $params, $cacheKey, true, $expiration);
    }

    /**
     * @TODO First look in indentityMap then cache ...
     * Load entities by id.
     * First get all from cache, then from DB
     * @return array of Entities
     */
    public function getMultiEntitiesByIds( array $entityId )
    {
        if( sizeof($entityId) == 0 || null == $entityId) {
            return array();
        }

        //from identity map
        $cachedIdentityMapVals = $this->getEntityMultiFromIdentityMap($this->entityClassName,$entityId);

        // -- fix for
        // $entityId = array_diff($entityId,array_keys($cachedIdentityMapVals));
        // because getEntityMultiFromIdentityMap returns array where keys just sequence value, but not entities ids
        if (!empty($cachedIdentityMapVals)) {
            // collect all entity ids in array
            $cachedIdentityMapIds = array();
            foreach ($cachedIdentityMapVals as $cachedIdentityMapEntity) {
                $cachedIdentityMapIds[] = $cachedIdentityMapEntity->getId();
            }
            $entityId = array_diff($entityId, $cachedIdentityMapIds);
        }
        // --

        $memcachedVals = Manager::getInstance()->getCache()->getMulti( $this->getEntitiesCacheKeyByListId($entityId) );
        $cachedVals = $cachedIdentityMapVals;
        if($memcachedVals){
            $cachedVals = array_merge($cachedVals,$memcachedVals);
        }

        $foundedId = array();
        if( null != $cachedVals ) {
            foreach ($cachedVals as $item) {
                $foundedId[] = $item->getId();
            }
        } else {
            $cachedVals = array();
        }
        $notFoundedId = array_diff($entityId, $foundedId);
        $dbVals = $this->loadEntitiesFromDbByIds($notFoundedId);
        if( sizeof($dbVals) != 0 ) {
            $this->saveListOfEntitiesInCache($dbVals);
        }
        $result = array_merge($cachedVals, $dbVals);
        return $result;

    }

    /**
     * Save list of entities in one request
     *
     * @param array[Entity] $entities
     * @return void
     */
    public function saveListOfEntitiesInCache( array $entities )
    {
        if(sizeof($entities)>0) {
            Manager::getInstance()->getCache()->setMulti($entities);
        }
    }

    protected function loadEntitiesFromDbByIds( array $entityId )
    {

        $result = array();
        if( sizeof($entityId) != 0 ) {
            $sql = 'SELECT *';
            $calculated = $this->getCalculatedExpressions();
            if(!empty($calculated))
                $sql.= ', '.implode(', ',$calculated);
            $sql.= ' FROM '.Manager::getInstance()->getDb()->quoteIdentifier($this->tableName).' WHERE '. $this->getIdField() .' IN ('. implode(",",$entityId) .')';
            $data = Manager::getInstance()->getDb()->fetchAll($sql);

            foreach ($data as $row) {
                $result[] = $this->createEntity($row);
            }
        }
        return $result;
    }

    /**
     *  returns an array containing fields that are not presented in the DB but are counted.
     *  For instance, array('COUNT(id)', 'SUM(rating) as overallrating')
     */
    protected function getCalculatedExpressions()
    {
        return array();
    }

    public function getCacheKeysByEntitiesId( array $ids )
    {
        $result = array();
        foreach ( $ids as $item ) {
            $result[] = $this->getEntityCacheKeyById($item);
        }
        return $result;
    }

    protected function getEntityCacheKeyById($id)
    {
        return $this->entityClassName . CacheProvider::KEY_DELIMITER . $id;
    }

    protected function getEntitiesCacheKeyByListId( array $ids)
    {
        $result = array();
        foreach ($ids as $item) {
            $result[] = $this->getEntityCacheKeyById($item);
        }
        return $result;
    }

    protected function getEntityCacheKeyByUniqueVals( array $values )
    {
        $key = $this->entityClassName . CacheProvider::KEY_DELIMITER;
        foreach ($values as $item) {
            if(is_string($item)) {
                $item = mb_strtolower($item);
            }
            $key .= CacheProvider::KEY_DELIMITER . $item;
        }
        return $key;
    }

    /**
     * Returns either Entity will be cached.
     * Since Entities don't have access to Data Layer, the have to call their Mapper's method
     * By default all entities are cacheable
     *
     * @param Entity $entity
     * @return bool
     */
    public function isCacheable(Entity $entity)
    {
        return true;
    }

    public function __toString()
    {
        return get_class($this);
    }

    /**
     * Generate default cache key name base on parent entity id and seed
     * @example usage (Entity_User $user, 'sort_by_rating') or (Entity_User $user, 'sort_by_num_posts')
     * @param Entity $entity
     * @param string $prefix Use it if you need different cache keys for same parent entity
     * @return string
     */
    public function getAggregateCacheKeyByParentEntity(Entity $entity=null, $prefix='')
    {
        $cacheKey = $this->aggregateCachePrefix;
        if( $prefix !== '' ) {
            $cacheKey .= $prefix . CacheProvider::KEY_DELIMITER;
        }
        if( null != $entity ) {
            $cacheKey .= get_class($entity) . CacheProvider::KEY_DELIMITER . $entity->getId();
        }
        return $cacheKey;
    }

    /**
     * Generate cache key basing on parent and child entity. Aggregate is replaced by concrete child id.
     * @param Entity $parent
     * @param Entity $child
     * @param string $prefix Use it if you need different cache keys for same parent entity
     * @return string
     */
    public function getAggregateCacheKeyByParentAndChildEntity(Entity $parent, Entity $child, $prefix = '')
    {
        $cacheKey = $this->getAggregateCacheKeyByParentEntity($parent,$child->getId()).CacheProvider::KEY_DELIMITER.$prefix;
        return $cacheKey;
    }

    /**
     * Generate cache key basing on entity list. Aggregate is replaced by concrete child id.
     * @param Aggregate|array $entityList
     * @param string $prefix Use it if you need different cache keys for same parent entity
     * @return string
     */
    public function getAggregateCacheKeyByEntities($entityList, $prefix = '')
    {
        $cacheKey = '';
        foreach($entityList as $entity){
            $cacheKey.= $this->getAggregateCacheKeyByParentEntity($entity).CacheProvider::KEY_DELIMITER;
        }
        return $cacheKey.$prefix;
    }

    /**
     * @param array $params
     * @return Entity
     * @throws BaseException
     */
    protected function getEntityFromDB( array $params )
    {
        //force set select *
        //$params[self::SQL_SELECT_TYPE] = self::SQL_SELECT_ALL;
        $data = $this->fetchArrayFromDB($params);
        if (count($data) > 1) {
            throw new BaseException('More than 1 row in result set');
        } elseif (count($data) == 0) {
            return null;
        }

        //So, count($data) == 1, it is our case :-)
        $proto = array_shift($data);
        return $this->createEntity($proto);
    }

    /**
     * Returns text of SQL query, that should be executed to fetch data from DB
     *
     * @param array $params
     * @return string
     */
    protected function createSelectStatement(array &$params)
    {
        $quoteSymbol = Manager::getInstance()->getDb()->getQuoteSymbol();

        $limit = $orderBy = $groupBy = '';
        if (isset($params[self::SQL_PARAM_LIMIT])) {
            $limit = ' LIMIT ' . $params[self::SQL_PARAM_LIMIT];
            unset($params[self::SQL_PARAM_LIMIT]);
        }

        if (isset($params[self::SQL_PARAM_ORDER_BY])) {
            $orderBy = ' ORDER BY ' . $params[self::SQL_PARAM_ORDER_BY];
            unset($params[self::SQL_PARAM_ORDER_BY]);
        }

        if (isset($params[self::SQL_PARAM_GROUP_BY])) {
            $groupBy = ' GROUP BY ' . $params[self::SQL_PARAM_GROUP_BY];
            unset($params[self::SQL_PARAM_GROUP_BY]);
        }

        $conditions = array();

        if (isset($params[self::SQL_PARAM_CONDITION])) {
            $pConditions = (array) $params[self::SQL_PARAM_CONDITION];
            foreach ($pConditions as $pCond) {
                $conditions[] = $pCond;
            }
            unset($params[self::SQL_PARAM_CONDITION]);
        }

        $keyCounter = 1;
        foreach ($params as $key => $value) {
            $conditions[] = $quoteSymbol . "{$key}" . $quoteSymbol . " = "  . $this->getPreparedParam($key, $keyCounter);
            $keyCounter++;
        }

        $sql = 'SELECT * FROM '.Manager::getInstance()->getDb()->quoteIdentifier($this->tableName) . (count($conditions) ?' WHERE ' . join(' AND ', $conditions) : '') . $groupBy . $orderBy . $limit;

        return $sql;
    }

    /**
     * Returns result set by means of which Entity will be generated
     *
     * @param array $params
     * @return array
     */
    protected function fetchArrayFromDB(array $params)
    {
        $sql = $this->createSelectStatement($params);
        //remove all auxiliary vars
        foreach ($params as $key => $value) {
            if( strpos($key, '_')===0 && strrpos($key, '_')===strlen($key)-1 ) {
                unset($params[$key]);
            }
        }
        return Manager::getInstance()->getDb()->fetchAll($sql, $params);
    }

    /**
     * Returns Entity by array. At first looks to IdentityMap, then creates new Entity
     *
     * @param array $proto
     * @return Entity
     */
    protected function getEntityFromIdentityMap($className, $id)
    {
        return Manager::getInstance()->getIdentityMap()->getEntity($className, $id);
    }

    /**
     *
     *
     * @param string $className
     * @param array $ids
     * @return array of Entity
     */
    protected function getEntityMultiFromIdentityMap($className, $ids)
    {
        return Manager::getInstance()->getIdentityMap()->getEntityMulti($className, $ids);
    }

    /**
     * Stores Aggregate or Entity in Cache
     * We don't differ Entities from Aggregates because the caching algorithm is the same
     * @see http://www.smira.ru/2008/10/29/web-caching-memcached-5/
     *
     * @param Business $object Entity or Aggregate object
     */
    protected function saveCached(Business $object)
    {
        $cacheKey = $object->getCacheKey();
        if (null !== $cacheKey) {

            $Cache = Manager::getInstance()->getCache();
            //reset only for entities
            if($object instanceof Entity) {
                $Cache->deleteEntity($object);
            }
            $Cache->set($object);
        }
    }

    /**
     * Contains loading of fields, that initialize after object initialization (lazy load).
     * Must be overriden in child Classes
     * In this abstract class only the simplest case is implemented only

     * @param Business $business
     * @param string $fieldName
     * @return mixed
     * @throws BaseException
     */
    public function lazyload($business, $fieldName)
    {
        if (false === $business instanceof Business) {
            throw new BaseException("Object `$business` is not instance of `Business` class");
        }

        if ($business instanceof Entity) {
            if (substr($fieldName, -3) == '_id') {
                //$name = ucfirst(substr($fieldName, 0, -3));
                 //fixed by A-25
                //mappers of field names with _ should have camelCase names
                //for example, street_type_id => Mapper_StreetType
                //or street_type_id => Mapper_Street_Type
                $name = substr($fieldName, 0, -3);
                $nameParts = explode('_',$name);


                foreach($nameParts as &$namePart)
                {
                    $namePart = ucfirst($namePart);
                }


                $name = implode('',$nameParts);
                $mapperClassName1Variant = 'Mapper_' . $name;
                $mapperClassName2Variant = 'Mapper_' . implode('_',$nameParts);
                if(class_exists($mapperClassName1Variant)){
                    $mapperClassName = $mapperClassName1Variant;
                } else {
                    //simply it was variant2
                    $mapperClassName = $mapperClassName2Variant;
                }

                if (class_exists($mapperClassName)) {
                    $mapper = Manager::getInstance()->getRepository()->get($mapperClassName);
                    $fieldValue = $business->getInfo($fieldName);

                    return $fieldValue !== null ? $mapper->getEntityById($fieldValue) : null;
                } else {
                   throw new BaseException("{$mapperClassName} not found");
                }
            }
        }

        throw new BaseException("Can't lazy load field {$fieldName} in mapper {$this}");
    }


    /**
     * @param array $uniqueKey
     * @param Entity $entity
     */
    protected function createUniqueFieldsCache(Entity $entity, array $uniqueKey)
    {
        if($this->hasUniqueFields()) {
            $key = $entity->getCacheKeyByUniqueFields($uniqueKey);
            Manager::getInstance()->getCache()->setRaw($key, $entity->getId());
        }
    }


    /**
     * @param Entity $entity
     */
    protected function createAllUniqueFieldsCache(Entity $entity)
    {
        if($this->hasUniqueFields()) {
            foreach ($this->uniqueFields as $uniqueKey) {
                $this->createUniqueFieldsCache($entity, $uniqueKey);
            }
        }
    }

    /**
     * Check if array contains all fields of any unique keys
     * and return first matched key or false if no key founded
     *
     * @param array $params
     * @return array|false
     */
    protected function getOneUniqueFromParams( array $params )
    {
        $result = false;

        if(!$this->hasUniqueFields()) {
            return false;
        }

        foreach ($this->uniqueFields as $uniqueKey) {
            $match = array();
            foreach ($params as $key => $val) {
                if(in_array($key, $uniqueKey)) {
                    $match[$key] = $val;
                }
            }
            if( sizeof($uniqueKey) === sizeof($match) ) {
                $result = $match;
                break;
            }
        }
        return $result;
    }

    /**
     * @param array $params
     * @return array
     */
    protected function getUniqueVals( array $params )
    {
        if(!$this->hasUniqueFields()) {
            return array();
        }
        $result = array();
        foreach ($params as $field => $val) {
            $result[] = $params[$field];
        }
        return $result;
    }


    /**
     * @return bool
     */
    protected function hasUniqueFields()
    {
        if(sizeof($this->uniqueFields)!=0) {
            return true;
        } else {
            return false;
        }
    }

    public function isIdAutoIncrement()
    {
        return true;
    }
}
