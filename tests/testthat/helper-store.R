test_seaweed_available <- function(url = "http://localhost:9333") {
  client <- kelp:::seaweed_client$new(url)
  available <- tryCatch({
    res <- client$GET("cluster/status")
    res$IsLeader
  }, error = function(e) FALSE)
  if (!available) {
    testthat::skip("Skipping test as seaweed is not available")
  }
  invisible(available)
}

skip_if_no_redis <- function() {
  skip_on_cran()
  tryCatch(
    redux::hiredis()$PING(),
    error = function(e) testthat::skip("redis not available"))
  invisible(NULL)
}

test_hiredis <- function() {
  skip_if_no_redis()
  redux::hiredis()
}

test_store <- function(..., prefix = NULL, cleanup = TRUE) {
  skip_if_not_installed("withr")
  prefix <- prefix %||% sprintf("rrq:test-store:%s", ids::random_id(1, 4))
  con <- test_hiredis()
  st <- rrq::object_store$new(con, prefix, ...)
  if (cleanup) {
    withr::defer_parent(st$destroy())
  }
  st
}

seaweed_master_url <- "http://localhost:9333"

get_kelp_ids <- function(hashes, queue_id) {
  ids <- vapply(rrq_kelp_hash_id(queue_id, hashes), function(redis_key) {
    kelp_id <- test_hiredis()$GET(redis_key)
    if (is.null(kelp_id)) {
      kelp_id <- NA_character_
    }
    kelp_id
  }, character(1))
  ## Some hashes might not have been uploaded to store
  ids[!is.na(ids)]
}
