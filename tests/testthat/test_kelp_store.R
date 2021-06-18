test_that("rrq can offload storage to SeaweedFS", {
  test_seaweed_available()
  con <- test_hiredis()
  queue_id <- ids::random_id()
  offload <- object_store_kelp$new(seaweed_master_url, con, queue_id)
  prefix <- sprintf("rrq:test-store:%s", ids::random_id(1, 4))
  s <- test_store(100, offload, prefix = prefix)

  t1 <- ids::random_id()
  t2 <- ids::random_id()
  x <- runif(20)
  y <- rnorm(20)
  z <- rexp(20)
  a <- list(1, x, 10, y)
  b <- list(2, y, 1, z)
  h1 <- s$mset(a, t1)
  h2 <- s$mset(b, t2)
  expect_equal(s$mget(h1), a)
  expect_equal(s$mget(h2), b)

  expect_setequal(s$list(), union(h1, h2))
  ids <- get_kelp_ids(union(h1, h2), queue_id)
  ## x, y and z have been offloaded
  expect_length(ids, 3)

  res <- s$drop(t1)

  expect_setequal(s$list(), h2)
  ids <- get_kelp_ids(union(h1, h2), queue_id)
  ## x and y are still in store
  expect_length(ids, 2)

  expect_error(s$mget(h1), "Some hashes were not found!")
  expect_equal(s$mget(h2), b)

  s$drop(t2)

  expect_setequal(s$list(), character(0))
  ids <- get_kelp_ids(union(h1, h2), queue_id)
  ## x and y removed
  expect_length(ids, 0)
  expect_setequal(
    redux::scan_find(con, sprintf("%s:tag_hash:*", prefix)),
    character(0))
})

test_that("trying to set different number of hashes and values fails", {
  test_seaweed_available()
  con <- test_hiredis()
  queue_id <- ids::random_id()
  offload <- object_store_kelp$new(seaweed_master_url, con, queue_id)
  expect_error(offload$mset("123", c(2, 3)), paste0(
               "Cannot store values, unequal number of hashes and values. ",
               "1 hash 2 values."),
               fixed = TRUE)
})

test_that("destroying removes all data", {
  test_seaweed_available()
  con <- test_hiredis()
  queue_id <- ids::random_id()
  offload <- object_store_kelp$new(seaweed_master_url, con, queue_id)
  s <- test_store(100, offload, cleanup = FALSE)

  t <- ids::random_id()
  x <- runif(20)
  h <- s$mset(list(pi, x), t)

  expect_setequal(s$list(), h)
  ids <- get_kelp_ids(h, queue_id)
  expect_length(ids, 1)
  kelp <- kelp::kelp$new(seaweed_master_url)
  data <- kelp$download_object(ids, collection = queue_id)
  expect_true(!is.null(data))

  s$destroy()

  expect_length(s$list(), 0)
  remaining_ids <- get_kelp_ids(h, queue_id)
  expect_length(remaining_ids, 0)
  expect_error(kelp$download_object(ids),
               "volume id \\d+ not found")
})
