// Parquet tables, read by Velox's Hive connector so that filters reach the
// reader: row groups skip on their statistics, unused subfields stay unread
// and joins push dynamic filters into scans. A table is a file or a directory
// of files on any registered file system; there is no catalog, and the table's
// schema comes from the Parquet footers. Text and gensort files stay on the
// file connector (Runtime.h).

#pragma once

namespace ember {

/// The connector behind read().parquet(). Its table schema is kParquetSchema
/// and its table names are paths, as for the file connector.
inline constexpr const char* kParquetConnectorId = "ember_parquet";
inline constexpr const char* kParquetSchema = "parquet";

/// Registers the connector, its metadata and serde. Needs the file connector's
/// metadata, which lists and validates the files. Safe to call more than once.
void registerParquetConnector();

} // namespace ember
