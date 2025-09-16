/**
 * Database Lifecycle Manager
 *
 * Manages database operations lifecycle including:
 * - Operation tracking for graceful shutdown
 * - Database state management
 * - Graceful closure with pending operation completion
 */

function DatabaseLifecycleManager (db) {
  var pendingOperations = new Set()
  var state = 'open' // 'open', 'closing', 'closed'
  var shutdownTimeout = 10000 // 10 seconds default timeout

  /**
   * Track a database operation to ensure graceful shutdown
   * @param {Promise} operation - The database operation promise
   * @returns {Promise} - The tracked operation promise
   */
  function trackOperation (operation) {
    if (state === 'closed') {
      return Promise.reject(new Error('Database is closed'))
    }

    // Add operation to tracking set
    pendingOperations.add(operation)

    // Remove operation when it completes (success or failure)
    function cleanup () {
      pendingOperations.delete(operation)
    }

    operation.then(cleanup, cleanup)

    return operation
  }

  /**
   * Get current database state
   * @returns {string} - Current state: 'open', 'closing', or 'closed'
   */
  function getState () {
    return state
  }

  /**
   * Get count of pending operations
   * @returns {number} - Number of pending operations
   */
  function getPendingOperationCount () {
    return pendingOperations.size
  }

  /**
   * Check if database is ready for operations
   * @returns {boolean} - True if database is ready
   */
  function isReady () {
    return state === 'open'
  }

  /**
   * Gracefully close the database, waiting for pending operations to complete
   * @param {Function} callback - Optional callback function
   * @returns {Promise} - Promise that resolves when database is closed
   */
  function gracefulClose (callback) {
    if (state === 'closed') {
      if (callback) {
        setImmediate(callback, null)
        return
      }
      return Promise.resolve()
    }

    if (state === 'closing') {
      // If already closing, wait for the existing close operation
      if (callback) {
        // Wait for state to become 'closed'
        function checkClosed () {
          if (state === 'closed') {
            setImmediate(callback, null)
          }
          else {
            setTimeout(checkClosed, 10)
          }
        }
        checkClosed()
        return
      }
      // Return a promise that resolves when closed
      return new Promise(function waitForClose (resolve) {
        function checkClosed () {
          if (state === 'closed') {
            resolve()
          }
          else {
            setTimeout(checkClosed, 10)
          }
        }
        checkClosed()
      })
    }

    state = 'closing'

    var closePromise = waitForOperationsAndClose()

    if (callback) {
      closePromise
        .then(function onGracefulCloseSuccess () {
          setImmediate(callback, null)
        })
        .catch(function onGracefulCloseError (err) {
          setImmediate(callback, err)
        })
    }

    return closePromise
  }

  /**
   * Wait for pending operations to complete, then close database
   * @private
   * @returns {Promise} - Promise that resolves when database is closed
   */
  function waitForOperationsAndClose () {
    return new Promise(function waitForOperationsPromise (resolve, reject) {
      // Wait for all pending operations to complete
      if (pendingOperations.size > 0) {
        waitForPendingOperations()
          .then(function onOperationsComplete () {
            closeDatabase()
              .then(resolve)
              .catch(reject)
          })
          .catch(reject)
      }
      else {
        closeDatabase()
          .then(resolve)
          .catch(reject)
      }
    })

    function closeDatabase () {
      return new Promise(function closeDatabasePromise (resolve, reject) {
        try {
          // Close the database using original close method
          var closePromise
          if (db.close._original) {
            closePromise = db.close._original()
          }
          else {
            closePromise = db.close()
          }

          closePromise
            .then(function onDatabaseClosed () {
              state = 'closed'
              resolve()
            })
            .catch(function onDatabaseCloseError (error) {
              state = 'closed' // Mark as closed even if there was an error
              reject(error)
            })
        }
        catch (error) {
          state = 'closed'
          reject(error)
        }
      })
    }
  }

  /**
   * Wait for all pending operations to complete with timeout
   * @private
   * @returns {Promise} - Promise that resolves when all operations complete
   */
  function waitForPendingOperations () {
    return new Promise(function waitForPendingPromise (resolve, reject) {
      var startTime = Date.now()

      function checkOperations () {
        if (pendingOperations.size === 0) {
          resolve()
          return
        }

        var elapsed = Date.now() - startTime
        if (elapsed >= shutdownTimeout) {
          reject(new Error('Shutdown timeout: ' + pendingOperations.size + ' operations still pending after ' + shutdownTimeout + 'ms'))
          return
        }

        // Check again in 10ms
        setTimeout(checkOperations, 10)
      }

      checkOperations()
    })
  }

  /**
   * Force close the database without waiting for operations
   * @param {Function} callback - Optional callback function
   * @returns {Promise} - Promise that resolves when database is closed
   */
  function forceClose (callback) {
    if (state === 'closed') {
      var err = new Error('Database is already closed')
      if (callback) {
        setImmediate(callback, err)
        return
      }
      return Promise.reject(err)
    }

    state = 'closed'
    pendingOperations.clear()

    var closePromise = db.close._original ?
      db.close._original() :
      db.close()

    if (callback) {
      closePromise
        .then(function onForceCloseSuccess () { setImmediate(callback, null) })
        .catch(function onForceCloseError (err) { setImmediate(callback, err) })
    }

    return closePromise
  }

  /**
   * Set shutdown timeout for graceful close operations
   * @param {number} timeout - Timeout in milliseconds
   */
  function setShutdownTimeout (timeout) {
    shutdownTimeout = timeout
  }

  /**
   * Get current shutdown timeout
   * @returns {number} - Timeout in milliseconds
   */
  function getShutdownTimeout () {
    return shutdownTimeout
  }

  return {
    trackOperation: trackOperation,
    getState: getState,
    getPendingOperationCount: getPendingOperationCount,
    isReady: isReady,
    gracefulClose: gracefulClose,
    forceClose: forceClose,
    setShutdownTimeout: setShutdownTimeout,
    getShutdownTimeout: getShutdownTimeout,
  }
}

module.exports = DatabaseLifecycleManager
