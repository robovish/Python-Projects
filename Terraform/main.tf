# resource "google_bigquery_dataset" "dataset" {
#   dataset_id                  = "example_dataset"
#   friendly_name               = "test"
#   description                 = "This is a test description"
#   location                    = "US"
#   default_table_expiration_ms = 3600000

#   labels = {
#     env = "dev"
#   }

# }

output "bq_dataset_id" {
    # value = "google_bigquery_dataset.dataset"
    value = "test"
}