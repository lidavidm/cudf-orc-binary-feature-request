#include <cstdlib>
#include <iostream>
#include <memory>

#include <arrow/adapters/orc/adapter.h>
#include <arrow/array/builder_binary.h>
#include <arrow/io/file.h>
#include <arrow/result.h>
#include <arrow/table.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <cudf/interop.hpp>
#include <cudf/io/orc.hpp>
#include <cudf/io/parquet.hpp>
#include <cudf/io/types.hpp>
#include <cudf/table/table.hpp>
#include <parquet/arrow/reader.h>

cudf::io::table_input_metadata MakeMetadata(cudf::table_view table_view) {
  cudf::io::table_input_metadata metadata(table_view);
  metadata.column_metadata[0].set_output_as_binary(true);
  return metadata;
}

void WriteOrc(cudf::table_view table) {
  auto metadata = MakeMetadata(table);
  auto sink = cudf::io::sink_info("/tmp/binary.orc");
  cudf::io::orc_writer_options out_opts =
      cudf::io::orc_writer_options::builder(sink, table)
          .metadata(std::move(metadata));
  cudf::io::write_orc(out_opts);
}

void WriteParquet(cudf::table_view table) {
  auto metadata = MakeMetadata(table);
  auto sink = cudf::io::sink_info("/tmp/binary.parquet");
  cudf::io::parquet_writer_options out_opts =
      cudf::io::parquet_writer_options::builder(sink, table)
          .metadata(std::move(metadata));
  cudf::io::write_parquet(out_opts);
}

arrow::Status MakeTable(std::shared_ptr<arrow::Table>* table) {
  arrow::StringBuilder builder;
  ARROW_RETURN_NOT_OK(builder.Append("Hello"));

  std::shared_ptr<arrow::Array> array;
  ARROW_RETURN_NOT_OK(builder.Finish(&array));

  auto schema = arrow::schema({arrow::field("binary", arrow::utf8())});
  *table = arrow::Table::Make(schema, {std::move(array)});
  return arrow::Status::OK();
}

arrow::Status ShowSchemaOrc(std::string path) {
  ARROW_ASSIGN_OR_RAISE(auto file, arrow::io::ReadableFile::Open(path));

  ARROW_ASSIGN_OR_RAISE(auto reader, arrow::adapters::orc::ORCFileReader::Open(
                                         file, arrow::default_memory_pool()));
  ARROW_ASSIGN_OR_RAISE(auto schema, reader->ReadSchema());

  std::cout << "Schema of " << path << std::endl;
  std::cout << schema->ToString() << std::endl;

  return arrow::Status::OK();
}

arrow::Status ShowSchemaParquet(std::string path) {
  ARROW_ASSIGN_OR_RAISE(auto file, arrow::io::ReadableFile::Open(path));

  std::unique_ptr<parquet::arrow::FileReader> reader;
  ARROW_RETURN_NOT_OK(
      parquet::arrow::OpenFile(file, arrow::default_memory_pool(), &reader));

  std::shared_ptr<arrow::Schema> schema;
  ARROW_RETURN_NOT_OK(reader->GetSchema(&schema));

  std::cout << "Schema of " << path << std::endl;
  std::cout << schema->ToString() << std::endl;

  return arrow::Status::OK();
}

int main(int argc, char** argv) {
  std::shared_ptr<arrow::Table> arrow_table;
  if (auto status = MakeTable(&arrow_table); !status.ok()) {
    std::cerr << status.ToString() << std::endl;
    return EXIT_FAILURE;
  }

  std::unique_ptr<cudf::table> table = cudf::from_arrow(*arrow_table);

  WriteOrc(*table);
  WriteParquet(*table);

  if (auto status = ShowSchemaOrc("/tmp/binary.orc"); !status.ok()) {
    std::cerr << status.ToString() << std::endl;
    return EXIT_FAILURE;
  }
  if (auto status = ShowSchemaParquet("/tmp/binary.parquet"); !status.ok()) {
    std::cerr << status.ToString() << std::endl;
    return EXIT_FAILURE;
  }

  return EXIT_SUCCESS;
}
