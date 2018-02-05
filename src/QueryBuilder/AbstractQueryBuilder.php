<?php
namespace SFM\QueryBuilder;

abstract class AbstractQueryBuilder
{
    protected $criteria;
    protected $sql = null;
    protected $params = array();

    /**
     * @return string
     */
    public function getSQL()
    {
        if (null === $this->sql) {
            $this->setup();
        }
        $this->prepareParams();

        return $this->sql;
    }

    /**
     * @return array
     */
    public function getParams()
    {
        if (null === $this->sql) {
            $this->setup();
        }
        $this->prepareParams();

        return $this->params;
    }
    
    protected function prepareParams()
    {
        if ($this->params) {
            foreach ($this->params as $param => $value) {
                if (is_bool($value)) {
                    $this->params[$param] = (int) $value;
                }
            }
        }
    }

    abstract protected function setup();
}
