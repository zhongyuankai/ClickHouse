#include <Storages/MergeTree/UniqueMergeTreeBlockOutputStream.h>
#include <Interpreters/Context.h>
#include <Poco/File.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
}


UniqueMergeTreeBlockOutputStream::UniqueMergeTreeBlockOutputStream(
    const MergeTreeDataPartPtr & data_part,
    const StorageMetadataPtr & metadata_snapshot_,
    const NamesAndTypesList & columns_list_,
    const MergeTreeIndices & skip_indices,
    CompressionCodecPtr default_codec,
    bool blocks_are_granules_size)
    : MergedBlockOutputStream(
        data_part,
        metadata_snapshot_,
        columns_list_)
{
    MergeTreeWriterSettings writer_settings(
        storage.global_context.getSettings(),
        data_part->index_granularity_info.is_adaptive,
        data_part->storage.global_context.getSettings().min_bytes_to_use_direct_io,
        blocks_are_granules_size);

    if (!part_path.empty())
        volume->getDisk()->createDirectories(part_path);

    writer = data_part->getWriter(columns_list, metadata_snapshot, skip_indices, default_codec, writer_settings);
    writer->initPrimaryIndex();
    writer->initSkipIndices();
}

/// If data is pre-sorted.
void UniqueMergeTreeBlockOutputStream::write(const Block & block)
{
    writeImpl(block, nullptr);
}

void UniqueMergeTreeBlockOutputStream::writeImpl(const Block & block, const IColumn::Permutation * permutation)
{
    block.checkNumberOfRows();
    size_t rows = block.rows();
    if (!rows)
        return;

    std::unordered_set<String> skip_indexes_column_names_set;
    for (const auto & index : metadata_snapshot->getSecondaryIndices())
        std::copy(index.column_names.cbegin(), index.column_names.cend(),
                std::inserter(skip_indexes_column_names_set, skip_indexes_column_names_set.end()));
    Names skip_indexes_column_names(skip_indexes_column_names_set.begin(), skip_indexes_column_names_set.end());

    Block primary_key_block = getBlockAndPermute(block, metadata_snapshot->getPrimaryKeyColumns(), permutation);
    Block skip_indexes_block = getBlockAndPermute(block, skip_indexes_column_names, permutation);

    writer->write(block, permutation, primary_key_block, skip_indexes_block);
    writer->calculateAndSerializeSkipIndices(skip_indexes_block);
    writer->calculateAndSerializePrimaryIndex(primary_key_block);
    writer->next();

    rows_count += rows;
}

}
