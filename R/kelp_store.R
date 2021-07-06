##' A SeaweedFS-based offload for [`rrq::object_store`]. This is not
##' intended at all for direct user-use.
##'
##' @title SeaweedFS-based rrq offload
##' @export
object_store_kelp <- R6::R6Class(
  "object_store_kelp",
  cloneable = FALSE,

  public = list(
    ##' @description Create the store
    ##'
    ##' @param seaweed_master_url URL to access seaweed master API
    ##' @param con A redis connection
    ##' @param queue_id Id of the queue, used as namespace for stored objects
    initialize = function(seaweed_master_url, con, queue_id) {
      private$kelp <- kelp::kelp$new(seaweed_master_url)
      private$con <- con
      private$queue_id <- queue_id
      private$hashes_set_id <- rrq_kelp_hashes(queue_id)
    },

    ##' @description Save a number of values to SeaweedFS
    ##'
    ##' @param hash A character vector of object hashes
    ##' @param value A list of serialised objects
    ##'   (each of which is a raw vector)
    mset = function(hash, value) {
      if (length(hash) != length(value)) {
        stop(sprintf(paste0(
          "Cannot store values, unequal number of hashes and values. ",
          "%s %s %s %s."),
          length(hash), ngettext(length(hash), "hash", "hashes"),
          length(value), ngettext(length(value), "value", "values")))
      }

      kelp_ids <- lapply(value, function(data) {
        private$kelp$upload_raw(data, collection = private$queue_id)
      })
      private$con$pipeline(
        redux::redis$MSET(rrq_kelp_hash_id(private$queue_id, hash), kelp_ids),
        redux::redis$SADD(private$hashes_set_id, hash)
      )
      invisible(TRUE)
    },

    ##' @description Retrieve a number of objects from SeaweedFS
    ##'
    ##' @param hash A character vector of hashes of the objects to return.
    ##'   The objects will be deserialised before return.
    mget = function(hash) {
      kelp_ids <- private$con$MGET(rrq_kelp_hash_id(private$queue_id, hash))
      lapply(kelp_ids, private$kelp$download_object,
             collection = private$queue_id)
    },

    ##' @description Delete a number of objects from SeaweedFS
    ##'
    ##' @param hash A character vector of hashes to remove
    mdel = function(hash) {
      kelp_ids <- private$con$MGET(rrq_kelp_hash_id(private$queue_id, hash))
      private$con$pipeline(
        redux::redis$SREM(private$hashes_set_id, hash),
        redux::redis$DEL(rrq_kelp_hash_id(private$queue_id, hash))
      )
      lapply(kelp_ids, private$kelp$delete, collection = private$queue_id)
    },

    ##' @description List hashes stored in this offload store
    list = function() {
      private$con$SMEMBERS(private$hashes_set_id)
    },

    ##' @description Completely delete the store by removing the entire
    ##' collection from SeaweedFS and removing all data from redis.
    destroy = function() {
      private$kelp$delete_collection(private$queue_id)
      del_hash_kelp_id_maps <- lapply(self$list(), function(x) {
        redux::redis$DEL(rrq_kelp_hash_id(private$queue_id, x))
      })
      del_hash_set <- list(redux::redis$DEL(private$hashes_set_id))
      private$con$pipeline(.commands = c(del_hash_kelp_id_maps,
                                         del_hash_set))
    }
  ),

  private = list(
    kelp = NULL,
    con = NULL,
    queue_id = NULL,
    hashes_set_id = NULL
  )
)
