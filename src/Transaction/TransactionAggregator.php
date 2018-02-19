<?php
namespace SFM\Transaction;

/**
 * Transaction engines aggregator.
 *
 * Provides synchronous way to control transaction through different layers
 */
class TransactionAggregator implements TransactionEngineInterface
{
    /**
     * @var TransactionEngineInterface[]
     */
    protected $engines = array();

    /**
     * @param TransactionEngineInterface $engine
     * @return $this
     */
    public function registerTransactionEngine(TransactionEngineInterface $engine)
    {
        $this->engines[] = $engine;

        return $this;
    }

    /**
     * Begin transaction

     * @throws TransactionException
     */
    public function beginTransaction()
    {

        /** Begin transaction on every registered engine */
        foreach ($this->engines as $engine) {
            $isActive = null;
            $exception = null;

            try {
                $engine->beginTransaction();
                $isActive = $engine->isTransaction();
            } catch (TransactionException $e) {
                $isActive = false;
                $exception = $e;
            }

            if (false === $isActive) {
                throw new TransactionException(sprintf("Can't begin transaction on `%s` engine", get_class($engine)), 0, $exception);
            }
        }
    }

    /**
     * @throws TransactionException
     */
    public function commitTransaction()
    {
        if ($this->isTransaction() === false) {
            throw new TransactionException("Can't commit transaction while there is no transaction running");
        }

        /** Begin transaction on every registered engine */
        foreach ($this->engines as $i => $engine) {
            $engine->commitTransaction();
        }
    }

    /**
     * @throws TransactionException
     */
    public function rollbackTransaction()
    {
        if ($this->isTransaction() === false) {
            throw new TransactionException("Can't rollback transaction while there is no transaction running");
        }

        /** Begin transaction on every registered engine */
        foreach ($this->engines as $i => $engine) {
            $engine->rollbackTransaction();
        }
    }

    /**
     * All transaction engines, registered in this aggregator,
     * must change transaction state synchronously

     * @throws TransactionException
     * @return bool
     */
    public function isTransaction()
    {
        $isTransactionStarted = null;
        foreach ($this->engines as $engine) {
            $isTransaction = $engine->isTransaction();

            // get state by first engine
            if ($isTransactionStarted === null) {
                $isTransactionStarted = $isTransaction;
                // all other must be in sync
            } else if ($isTransaction !== $isTransactionStarted) {
                throw new TransactionException(sprintf("Transaction engine `%s` is desynchronized from other last engine", get_class($engine)));
            }
        }

        return $isTransactionStarted;
    }
}
