rrq_kelp_hash_id <- function(queue_id, hash) {
  sprintf("rrq:kelp:%s:hash::%s:id", queue_id, hash)
}

rrq_kelp_hashes <- function(queue_id) {
  sprintf("rrq:kelp:%s:hashes", queue_id)
}
