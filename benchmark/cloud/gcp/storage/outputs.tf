output "bucket_name" {
  value = google_storage_bucket.corpus.name
}

output "bucket_uri" {
  value = google_storage_bucket.corpus.url
}
