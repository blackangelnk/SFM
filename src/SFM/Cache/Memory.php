<?php

/**
 *  Class for work with Memcached in memory. Implements tags system for cache control
 */
class SFM_Cache_Memory extends SFM_Cache implements SFM_Transaction_Engine, SFM_Cache_Interface
{
    public function setValue($key, SFM_Value $value, $expiration = 0)
    {
        $this->setRaw($key, $value->get(), $expiration);
        $this->transaction->logResetable($value);
    }
    
    public function getRaw($key)
    {
        $value = $this->driver->get($this->generateKey($key));
        return ($value === false) ? null : $value;
    }
    
    public function incrementRaw($key)
    {
        return $this->driver->increment($this->generateKey($key));
    }

    public function decrementRaw($key)
    {
        return $this->driver->decrement($this->generateKey($key));
    }
    
    public function deleteRaw($key)
    {
        return $this->driver->delete($this->generateKey($key));
    }
    
    public function getResultCode()
    {
        return $this->driver->getResultCode();
    }
}    