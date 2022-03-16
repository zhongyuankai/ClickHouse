#include <boost/rational.hpp>   /// For calculations related to sampling coefficients.
#include <ext/scope_guard.h>
#include <optional>

#include <Poco/File.h>

#include <Common/FieldVisitors.h>
#include <Storages/MergeTree/UniqueMergeTreeDataSelectExecutor.h>
#include <Storages/MergeTree/UniqueMergeTreeSelectProcessor.h>
#include <Storages/MergeTree/UniqueMergeTreeReverseSelectProcessor.h>
#include <Storages/MergeTree/UniqueMergeTreeReadPool.h>
#include <Storages/MergeTree/UniqueMergeTreeThreadSelectBlockInputProcessor.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/ReadInOrderOptimizer.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSampleRatio.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/Context.h>

/// Allow to use __uint128_t as a template parameter for boost::rational.
// https://stackoverflow.com/questions/41198673/uint128-t-not-working-with-clang-and-libstdc
#if 0
#if !defined(__GLIBCXX_BITSIZE_INT_N_0) && defined(__SIZEOF_INT128__)
namespace std
{
    template <>
    struct numeric_limits<__uint128_t>
    {
        static constexpr bool is_specialized = true;
        static constexpr bool is_signed = false;
        static constexpr bool is_integer = true;
        static constexpr int radix = 2;
        static constexpr int digits = 128;
        static constexpr int digits10 = 38;
        static constexpr __uint128_t min () { return 0; } // used in boost 1.65.1+
        static constexpr __uint128_t max () { return __uint128_t(0) - 1; } // used in boost 1.68.0+
    };
}
#endif
#endif

#include <DataStreams/CreatingSetsBlockInputStream.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/ConcatProcessor.h>
#include <Processors/Merges/AggregatingSortedTransform.h>
#include <Processors/Merges/CollapsingSortedTransform.h>
#include <Processors/Merges/MergingSortedTransform.h>
#include <Processors/Merges/ReplacingSortedTransform.h>
#include <Processors/Merges/SummingSortedTransform.h>
#include <Processors/Merges/VersionedCollapsingTransform.h>
#include <Processors/Merges/UniqueSortedTransform.h>
#include <Processors/Sources/SourceFromInputStream.h>
#include <Processors/Transforms/AddingConstColumnTransform.h>
#include <Processors/Transforms/AddingSelectorTransform.h>
#include <Processors/Transforms/CopyTransform.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/FilterTransform.h>
#include <Processors/Transforms/ReverseTransform.h>
#include <Storages/VirtualColumnUtils.h>

namespace ProfileEvents
{
    extern const Event SelectedParts;
    extern const Event SelectedRanges;
    extern const Event SelectedMarks;
}


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INDEX_NOT_USED;
    extern const int ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER;
    extern const int ILLEGAL_COLUMN;
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int TOO_MANY_ROWS;
}


UniqueMergeTreeDataSelectExecutor::UniqueMergeTreeDataSelectExecutor(StorageReplicatedUniqueMergeTree &data_)
    :  MergeTreeDataSelectExecutor(data_), storage_data(data_)
{
}

Pipe UniqueMergeTreeDataSelectExecutor::readFromParts(
    MergeTreeData::DataPartsVector parts,
    const Names & column_names_to_return,
    const StorageMetadataPtr & metadata_snapshot,
    const SelectQueryInfo & query_info,
    const Context & context,
    const UInt64 max_block_size,
    const unsigned num_streams,
    const PartitionIdToMaxBlock * max_block_numbers_to_read) const
{
    /// If query contains restrictions on the virtual column `_part` or `_part_index`, select only parts suitable for it.
    /// The virtual column `_sample_factor` (which is equal to 1 / used sample rate) can be requested in the query.
    Names virt_column_names;
    Names real_column_names;

    bool part_column_queried = false;

    bool sample_factor_column_queried = false;
    Float64 used_sample_factor = 1;

    for (const String & name : column_names_to_return)
    {
        if (name == "_part")
        {
            part_column_queried = true;
            virt_column_names.push_back(name);
        }
        else if (name == "_part_index")
        {
            virt_column_names.push_back(name);
        }
        else if (name == "_partition_id")
        {
            virt_column_names.push_back(name);
        }
        else if (name == "_sample_factor")
        {
            sample_factor_column_queried = true;
            virt_column_names.push_back(name);
        }
        else if (name == "_unique_key_id")
        {
        }
        else
        {
            real_column_names.push_back(name);
        }
    }

    if (data.merging_params.mode == MergeTreeData::MergingParams::Unique) {
        real_column_names.push_back("_unique_key_id");
    }

    NamesAndTypesList available_real_columns = metadata_snapshot->getColumns().getAllPhysical();

    /// If there are only virtual columns in the query, you must request at least one non-virtual one.
    if (real_column_names.empty())
        real_column_names.push_back(ExpressionActions::getSmallestColumn(available_real_columns));

    /// If `_part` virtual column is requested, we try to use it as an index.
    Block virtual_columns_block = getBlockWithPartColumn(parts);
    if (part_column_queried)
        VirtualColumnUtils::filterBlockWithQuery(query_info.query, virtual_columns_block, context);

    std::multiset<String> part_values = VirtualColumnUtils::extractSingleValueFromBlock<String>(virtual_columns_block, "_part");

    metadata_snapshot->check(real_column_names, data.getVirtuals(), data.getStorageID());

    const Settings & settings = context.getSettingsRef();
    const auto & primary_key = metadata_snapshot->getPrimaryKey();
    Names primary_key_columns = primary_key.column_names;

    KeyCondition key_condition(query_info, context, primary_key_columns, primary_key.expression);

    if (settings.force_primary_key && key_condition.alwaysUnknownOrTrue())
    {
        std::stringstream exception_message;
        exception_message << "Primary key (";
        for (size_t i = 0, size = primary_key_columns.size(); i < size; ++i)
            exception_message << (i == 0 ? "" : ", ") << primary_key_columns[i];
        exception_message << ") is not used and setting 'force_primary_key' is set.";

        throw Exception(exception_message.str(), ErrorCodes::INDEX_NOT_USED);
    }

    std::optional<KeyCondition> minmax_idx_condition;
    if (data.minmax_idx_expr)
    {
        minmax_idx_condition.emplace(query_info, context, data.minmax_idx_columns, data.minmax_idx_expr);

        if (settings.force_index_by_date && minmax_idx_condition->alwaysUnknownOrTrue())
        {
            String msg = "MinMax index by columns (";
            bool first = true;
            for (const String & col : data.minmax_idx_columns)
            {
                if (first)
                    first = false;
                else
                    msg += ", ";
                msg += col;
            }
            msg += ") is not used and setting 'force_index_by_date' is set";

            throw Exception(msg, ErrorCodes::INDEX_NOT_USED);
        }
    }

    /// Select the parts in which there can be data that satisfy `minmax_idx_condition` and that match the condition on `_part`,
    ///  as well as `max_block_number_to_read`.
    std::map<String ,PartBitmap::Ptr> part_bitmaps;
    {
        auto prev_parts = parts;
        parts.clear();

        const String * prev_partition_id = nullptr;
        MergeTreeData::DataPartsVector partition_parts;
        MergeTreeData::DataPartsVector partition_select_parts;
        std::vector<PartBitmap::Ptr> partition_part_bitmaps;

        auto merge_partition_parts = [&]() {
            assert(partition_parts.size() >= partition_select_parts.size());
            storage_data.mergePartsBitmaps(partition_parts, partition_select_parts, partition_part_bitmaps);
            parts.insert(parts.end(), partition_select_parts.begin(), partition_select_parts.end());
            for (auto &part_bitmap: partition_part_bitmaps) {
                part_bitmaps.insert(std::make_pair(part_bitmap->part->name, part_bitmap));
            }

        };

        for (const auto & part : prev_parts)
        {
            if (prev_partition_id == nullptr) prev_partition_id  = &part->info.partition_id;
            if (*prev_partition_id != part->info.partition_id) {
                merge_partition_parts();

                prev_partition_id = &part->info.partition_id;
                partition_part_bitmaps.clear();
                partition_parts.clear();
                partition_select_parts.clear();
            }

            partition_parts.push_back(part);

            if (part_values.find(part->name) == part_values.end())
                continue;

            if (part->isEmpty())
                continue;

            if (minmax_idx_condition && !minmax_idx_condition->checkInHyperrectangle(
                    part->minmax_idx.hyperrectangle, data.minmax_idx_column_types).can_be_true)
                continue;

            if (max_block_numbers_to_read)
            {
                auto blocks_iterator = max_block_numbers_to_read->find(part->info.partition_id);
                if (blocks_iterator == max_block_numbers_to_read->end() || part->info.max_block > blocks_iterator->second)
                    continue;
            }

            partition_select_parts.push_back(part);
        }
        merge_partition_parts();
    }
    assert(parts.size() == part_bitmaps.size());

    Names column_names_to_read = real_column_names;
    std::shared_ptr<ASTFunction> filter_function;
    ExpressionActionsPtr filter_expression;
    const auto & select = query_info.query->as<ASTSelectQuery &>();

    LOG_DEBUG(log, "Key condition: {}", key_condition.toString());
    if (minmax_idx_condition)
        LOG_DEBUG(log, "MinMax index condition: {}", minmax_idx_condition->toString());

    MergeTreeReaderSettings reader_settings =
    {
        .min_bytes_to_use_direct_io = settings.min_bytes_to_use_direct_io,
        .min_bytes_to_use_mmap_io = settings.min_bytes_to_use_mmap_io,
        .max_read_buffer_size = settings.max_read_buffer_size,
        .save_marks_in_cache = true
    };

    /// PREWHERE
    String prewhere_column;
    if (select.prewhere())
        prewhere_column = select.prewhere()->getColumnName();

    std::vector<std::pair<MergeTreeIndexPtr, MergeTreeIndexConditionPtr>> useful_indices;

    for (const auto & index : metadata_snapshot->getSecondaryIndices())
    {
        auto index_helper = MergeTreeIndexFactory::instance().get(index);
        auto condition = index_helper->createIndexCondition(query_info, context);
        if (!condition->alwaysUnknownOrTrue())
            useful_indices.emplace_back(index_helper, condition);
    }

    RangesInDataParts parts_with_ranges(parts.size());
    size_t sum_marks = 0;
    std::atomic<size_t> sum_marks_pk = 0;
    size_t sum_ranges = 0;

    /// Let's find what range to read from each part.
    {
        std::atomic<size_t> total_rows {0};

        auto process_part = [&](size_t part_index)
        {
            auto & part = parts[part_index];

            RangesInDataPart ranges(part, part_index);

            if (metadata_snapshot->hasPrimaryKey())
                ranges.ranges = markRangesFromPKRange(part, metadata_snapshot, key_condition, settings, log);
            else
            {
                size_t total_marks_count = part->getMarksCount();
                if (total_marks_count)
                {
                    if (part->index_granularity.hasFinalMark())
                        --total_marks_count;
                    ranges.ranges = MarkRanges{MarkRange{0, total_marks_count}};
                }
            }

            sum_marks_pk.fetch_add(ranges.getMarksCount(), std::memory_order_relaxed);

            for (const auto & index_and_condition : useful_indices)
                ranges.ranges = filterMarksUsingIndex(
                        index_and_condition.first, index_and_condition.second, part, ranges.ranges, settings, reader_settings, log);

            if (!ranges.ranges.empty())
            {
                if (settings.read_overflow_mode == OverflowMode::THROW && settings.max_rows_to_read)
                {
                    /// Fail fast if estimated number of rows to read exceeds the limit
                    auto current_rows_estimate = ranges.getRowsCount();
                    size_t prev_total_rows_estimate = total_rows.fetch_add(current_rows_estimate);
                    size_t total_rows_estimate = current_rows_estimate + prev_total_rows_estimate;
                    if (total_rows_estimate > settings.max_rows_to_read)
                        throw Exception(
                            "Limit for rows (controlled by 'max_rows_to_read' setting) exceeded, max rows: "
                            + formatReadableQuantity(settings.max_rows_to_read)
                            + ", estimated rows to read (at least): " + formatReadableQuantity(total_rows_estimate),
                            ErrorCodes::TOO_MANY_ROWS);
                }

                parts_with_ranges[part_index] = std::move(ranges);
            }
        };

        size_t num_threads = std::min(size_t(num_streams), parts.size());

        if (num_threads <= 1)
        {
            for (size_t part_index = 0; part_index < parts.size(); ++part_index)
                process_part(part_index);
        }
        else
        {
            /// Parallel loading of data parts.
            ThreadPool pool(num_threads);

            for (size_t part_index = 0; part_index < parts.size(); ++part_index)
                pool.scheduleOrThrowOnError([&, part_index, thread_group = CurrentThread::getGroup()] {
                    SCOPE_EXIT(
                        if (thread_group)
                            CurrentThread::detachQueryIfNotDetached();
                    );
                    if (thread_group)
                        CurrentThread::attachTo(thread_group);

                    process_part(part_index);
                });

            pool.wait();
        }

        /// Skip empty ranges.
        size_t next_part = 0;
        for (size_t part_index = 0; part_index < parts.size(); ++part_index)
        {
            auto & part = parts_with_ranges[part_index];
            if (!part.data_part)
                continue;

            sum_ranges += part.ranges.size();
            sum_marks += part.getMarksCount();

            if (next_part != part_index)
                std::swap(parts_with_ranges[next_part], part);

            ++next_part;
        }

        parts_with_ranges.resize(next_part);
    }

    LOG_DEBUG(log, "Selected {} parts by date, {} parts by key, {} marks by primary key, {} marks to read from {} ranges", parts.size(), parts_with_ranges.size(), sum_marks_pk.load(std::memory_order_relaxed), sum_marks, sum_ranges);

    if (parts_with_ranges.empty())
        return {};

    ProfileEvents::increment(ProfileEvents::SelectedParts, parts_with_ranges.size());
    ProfileEvents::increment(ProfileEvents::SelectedRanges, sum_ranges);
    ProfileEvents::increment(ProfileEvents::SelectedMarks, sum_marks);

    Pipe res;

    /// Projection, that needed to drop columns, which have appeared by execution
    /// of some extra expressions, and to allow execute the same expressions later.
    /// NOTE: It may lead to double computation of expressions.
    ExpressionActionsPtr result_projection;

    if ((settings.optimize_read_in_order || settings.optimize_aggregation_in_order) && query_info.input_order_info)
    {
        size_t prefix_size = query_info.input_order_info->order_key_prefix_descr.size();
        auto order_key_prefix_ast = metadata_snapshot->getSortingKey().expression_list_ast->clone();
        order_key_prefix_ast->children.resize(prefix_size);

        auto syntax_result = TreeRewriter(context).analyze(order_key_prefix_ast, metadata_snapshot->getColumns().getAllPhysical());
        auto sorting_key_prefix_expr = ExpressionAnalyzer(order_key_prefix_ast, syntax_result, context).getActions(false);

        res = spreadMarkRangesAmongStreamsWithOrder(
            std::move(parts_with_ranges),
            num_streams,
            column_names_to_read,
            metadata_snapshot,
            max_block_size,
            settings.use_uncompressed_cache,
            query_info,
            sorting_key_prefix_expr,
            virt_column_names,
            settings,
            reader_settings,
            result_projection,
            part_bitmaps);
    }
    else
    {
        res = spreadMarkRangesAmongStreams(
            std::move(parts_with_ranges),
            num_streams,
            column_names_to_read,
            metadata_snapshot,
            max_block_size,
            settings.use_uncompressed_cache,
            query_info,
            virt_column_names,
            settings,
            reader_settings,
            part_bitmaps);
    }

    if (result_projection)
    {
        res.addSimpleTransform([&result_projection](const Block & header)
        {
            return std::make_shared<ExpressionTransform>(header, result_projection);
        });
    }

    /// By the way, if a distributed query or query to a Merge table is made, then the `_sample_factor` column can have different values.
    if (sample_factor_column_queried)
    {
        res.addSimpleTransform([used_sample_factor](const Block & header)
        {
            return std::make_shared<AddingConstColumnTransform<Float64>>(
                    header, std::make_shared<DataTypeFloat64>(), used_sample_factor, "_sample_factor");
        });
    }

    if (query_info.prewhere_info && query_info.prewhere_info->remove_columns_actions)
    {
        res.addSimpleTransform([&query_info](const Block & header)
        {
            return std::make_shared<ExpressionTransform>(header, query_info.prewhere_info->remove_columns_actions);
        });
    }

    return res;
}

Pipe UniqueMergeTreeDataSelectExecutor::spreadMarkRangesAmongStreams(
    RangesInDataParts && parts,
    size_t num_streams,
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    UInt64 max_block_size,
    bool use_uncompressed_cache,
    const SelectQueryInfo & query_info,
    const Names & virt_columns,
    const Settings & settings,
    const MergeTreeReaderSettings & reader_settings,
    const std::map<String, PartBitmap::Ptr> &part_bitmaps) const
{
    /// Count marks for each part.
    std::vector<size_t> sum_marks_in_parts(parts.size());
    size_t sum_marks = 0;
    size_t total_rows = 0;

    const auto data_settings = data.getSettings();
    size_t adaptive_parts = 0;
    for (size_t i = 0; i < parts.size(); ++i)
    {
        total_rows += parts[i].getRowsCount();
        sum_marks_in_parts[i] = parts[i].getMarksCount();
        sum_marks += sum_marks_in_parts[i];

        if (parts[i].data_part->index_granularity_info.is_adaptive)
            ++adaptive_parts;
    }

    size_t index_granularity_bytes = 0;
    if (adaptive_parts > parts.size() / 2)
        index_granularity_bytes = data_settings->index_granularity_bytes;

    const size_t max_marks_to_use_cache = roundRowsOrBytesToMarks(
        settings.merge_tree_max_rows_to_use_cache,
        settings.merge_tree_max_bytes_to_use_cache,
        data_settings->index_granularity,
        index_granularity_bytes);

    const size_t min_marks_for_concurrent_read = roundRowsOrBytesToMarks(
        settings.merge_tree_min_rows_for_concurrent_read,
        settings.merge_tree_min_bytes_for_concurrent_read,
        data_settings->index_granularity,
        index_granularity_bytes);

    if (sum_marks > max_marks_to_use_cache)
        use_uncompressed_cache = false;

    if (0 == sum_marks)
        return {};

    if (num_streams > 1)
    {
        /// Parallel query execution.
        Pipes res;

        /// Reduce the number of num_streams if the data is small.
        if (sum_marks < num_streams * min_marks_for_concurrent_read && parts.size() < num_streams)
            num_streams = std::max((sum_marks + min_marks_for_concurrent_read - 1) / min_marks_for_concurrent_read, parts.size());

        UniqueMergeTreeReadPoolPtr pool = std::make_shared<UniqueMergeTreeReadPool>(
            part_bitmaps,
            num_streams,
            sum_marks,
            min_marks_for_concurrent_read,
            parts,
            data,
            metadata_snapshot,
            query_info.prewhere_info,
            true,
            column_names,
            MergeTreeReadPool::BackoffSettings(settings),
            settings.preferred_block_size_bytes,
            false);

        /// Let's estimate total number of rows for progress bar.
        LOG_TRACE(log, "Reading approx. {} rows with {} streams", total_rows, num_streams);

        for (size_t i = 0; i < num_streams; ++i)
        {
            auto source = std::make_shared<UniqueMergeTreeThreadSelectBlockInputProcessor>(
                i, pool, min_marks_for_concurrent_read, max_block_size,
                settings.preferred_block_size_bytes, settings.preferred_max_column_in_block_size_bytes,
                data, metadata_snapshot, use_uncompressed_cache,
                query_info.prewhere_info, reader_settings, virt_columns);

            if (i == 0)
            {
                /// Set the approximate number of rows for the first source only
                source->addTotalRowsApprox(total_rows);
            }

            res.emplace_back(std::move(source));
        }

        return Pipe::unitePipes(std::move(res));
    }
    else
    {
        /// Sequential query execution.
        Pipes res;

        for (const auto & part : parts)
        {
            auto source = std::make_shared<UniqueMergeTreeSelectProcessor>(
                data, metadata_snapshot, part.data_part, max_block_size, settings.preferred_block_size_bytes,
                settings.preferred_max_column_in_block_size_bytes, column_names, part.ranges, use_uncompressed_cache,
                query_info.prewhere_info, true, reader_settings, part_bitmaps.at(part.data_part->name), virt_columns, part.part_index_in_query);

            res.emplace_back(std::move(source));
        }

        auto pipe = Pipe::unitePipes(std::move(res));

        /// Use ConcatProcessor to concat sources together.
        /// It is needed to read in parts order (and so in PK order) if single thread is used.
        if (pipe.numOutputPorts() > 1)
            pipe.addTransform(std::make_shared<ConcatProcessor>(pipe.getHeader(), pipe.numOutputPorts()));

        return pipe;
    }
}

Pipe UniqueMergeTreeDataSelectExecutor::spreadMarkRangesAmongStreamsWithOrder(
    RangesInDataParts && parts,
    size_t num_streams,
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    UInt64 max_block_size,
    bool use_uncompressed_cache,
    const SelectQueryInfo & query_info,
    const ExpressionActionsPtr & sorting_key_prefix_expr,
    const Names & virt_columns,
    const Settings & settings,
    const MergeTreeReaderSettings & reader_settings,
    ExpressionActionsPtr & out_projection,
    const std::map<String, PartBitmap::Ptr> &part_bitmaps) const
{
    size_t sum_marks = 0;
    const InputOrderInfoPtr & input_order_info = query_info.input_order_info;

    size_t adaptive_parts = 0;
    std::vector<size_t> sum_marks_in_parts(parts.size());
    const auto data_settings = data.getSettings();

    for (size_t i = 0; i < parts.size(); ++i)
    {
        sum_marks_in_parts[i] = parts[i].getMarksCount();
        sum_marks += sum_marks_in_parts[i];

        if (parts[i].data_part->index_granularity_info.is_adaptive)
            ++adaptive_parts;
    }

    size_t index_granularity_bytes = 0;
    if (adaptive_parts > parts.size() / 2)
        index_granularity_bytes = data_settings->index_granularity_bytes;

    const size_t max_marks_to_use_cache = roundRowsOrBytesToMarks(
        settings.merge_tree_max_rows_to_use_cache,
        settings.merge_tree_max_bytes_to_use_cache,
        data_settings->index_granularity,
        index_granularity_bytes);

    const size_t min_marks_for_concurrent_read = roundRowsOrBytesToMarks(
        settings.merge_tree_min_rows_for_concurrent_read,
        settings.merge_tree_min_bytes_for_concurrent_read,
        data_settings->index_granularity,
        index_granularity_bytes);

    if (sum_marks > max_marks_to_use_cache)
        use_uncompressed_cache = false;

    Pipes res;

    if (sum_marks == 0)
        return {};

    /// Let's split ranges to avoid reading much data.
    auto split_ranges = [rows_granularity = data_settings->index_granularity, max_block_size](const auto & ranges, int direction)
    {
        MarkRanges new_ranges;
        const size_t max_marks_in_range = (max_block_size + rows_granularity - 1) / rows_granularity;
        size_t marks_in_range = 1;

        if (direction == 1)
        {
            /// Split first few ranges to avoid reading much data.
            bool split = false;
            for (auto range : ranges)
            {
                while (!split && range.begin + marks_in_range < range.end)
                {
                    new_ranges.emplace_back(range.begin, range.begin + marks_in_range);
                    range.begin += marks_in_range;
                    marks_in_range *= 2;

                    if (marks_in_range > max_marks_in_range)
                        split = true;
                }
                new_ranges.emplace_back(range.begin, range.end);
            }
        }
        else
        {
            /// Split all ranges to avoid reading much data, because we have to
            ///  store whole range in memory to reverse it.
            for (auto it = ranges.rbegin(); it != ranges.rend(); ++it)
            {
                auto range = *it;
                while (range.begin + marks_in_range < range.end)
                {
                    new_ranges.emplace_front(range.end - marks_in_range, range.end);
                    range.end -= marks_in_range;
                    marks_in_range = std::min(marks_in_range * 2, max_marks_in_range);
                }
                new_ranges.emplace_front(range.begin, range.end);
            }
        }

        return new_ranges;
    };

    const size_t min_marks_per_stream = (sum_marks - 1) / num_streams + 1;
    bool need_preliminary_merge = (parts.size() > settings.read_in_order_two_level_merge_threshold);
    size_t max_output_ports = 0;

    for (size_t i = 0; i < num_streams && !parts.empty(); ++i)
    {
        size_t need_marks = min_marks_per_stream;

        Pipes pipes;

        /// Loop over parts.
        /// We will iteratively take part or some subrange of a part from the back
        ///  and assign a stream to read from it.
        while (need_marks > 0 && !parts.empty())
        {
            RangesInDataPart part = parts.back();
            parts.pop_back();

            size_t & marks_in_part = sum_marks_in_parts.back();

            /// We will not take too few rows from a part.
            if (marks_in_part >= min_marks_for_concurrent_read &&
                need_marks < min_marks_for_concurrent_read)
                need_marks = min_marks_for_concurrent_read;

            /// Do not leave too few rows in the part.
            if (marks_in_part > need_marks &&
                marks_in_part - need_marks < min_marks_for_concurrent_read)
                need_marks = marks_in_part;

            MarkRanges ranges_to_get_from_part;

            /// We take the whole part if it is small enough.
            if (marks_in_part <= need_marks)
            {
                ranges_to_get_from_part = part.ranges;

                need_marks -= marks_in_part;
                sum_marks_in_parts.pop_back();
            }
            else
            {
                /// Loop through ranges in part. Take enough ranges to cover "need_marks".
                while (need_marks > 0)
                {
                    if (part.ranges.empty())
                        throw Exception("Unexpected end of ranges while spreading marks among streams", ErrorCodes::LOGICAL_ERROR);

                    MarkRange & range = part.ranges.front();

                    const size_t marks_in_range = range.end - range.begin;
                    const size_t marks_to_get_from_range = std::min(marks_in_range, need_marks);

                    ranges_to_get_from_part.emplace_back(range.begin, range.begin + marks_to_get_from_range);
                    range.begin += marks_to_get_from_range;
                    marks_in_part -= marks_to_get_from_range;
                    need_marks -= marks_to_get_from_range;
                    if (range.begin == range.end)
                        part.ranges.pop_front();
                }
                parts.emplace_back(part);
            }
            ranges_to_get_from_part = split_ranges(ranges_to_get_from_part, input_order_info->direction);

            if (input_order_info->direction == 1)
            {
                pipes.emplace_back(std::make_shared<UniqueMergeTreeSelectProcessor>(
                    data,
                    metadata_snapshot,
                    part.data_part,
                    max_block_size,
                    settings.preferred_block_size_bytes,
                    settings.preferred_max_column_in_block_size_bytes,
                    column_names,
                    ranges_to_get_from_part,
                    use_uncompressed_cache,
                    query_info.prewhere_info,
                    true,
                    reader_settings,
                    part_bitmaps.at(part.data_part->name),
                    virt_columns,
                    part.part_index_in_query));
            }
            else
            {
                pipes.emplace_back(std::make_shared<UniqueMergeTreeReverseSelectProcessor>(
                    data,
                    metadata_snapshot,
                    part.data_part,
                    max_block_size,
                    settings.preferred_block_size_bytes,
                    settings.preferred_max_column_in_block_size_bytes,
                    column_names,
                    ranges_to_get_from_part,
                    use_uncompressed_cache,
                    query_info.prewhere_info,
                    true,
                    reader_settings,
                    part_bitmaps.at(part.data_part->name),
                    virt_columns,
                    part.part_index_in_query));
            }
        }

        auto pipe = Pipe::unitePipes(std::move(pipes));

        if (input_order_info->direction != 1)
        {
            pipe.addSimpleTransform([](const Block & header)
                                    {
                                        return std::make_shared<ReverseTransform>(header);
                                    });
        }

        max_output_ports = std::max(pipe.numOutputPorts(), max_output_ports);
        res.emplace_back(std::move(pipe));
    }

    if (need_preliminary_merge)
    {
        /// If ORDER BY clause of the query contains some expression,
        /// then those new columns should be added for the merge step,
        /// and this should be done always, if there is at least one pipe that
        /// has multiple output ports.
        bool sorting_key_has_expression = sortingDescriptionHasExpressions(input_order_info->order_key_prefix_descr, metadata_snapshot);
        bool force_sorting_key_transform = res.size() > 1 && max_output_ports > 1 && sorting_key_has_expression;

        for (auto & pipe : res)
        {
            SortDescription sort_description;

            if (pipe.numOutputPorts() > 1 || force_sorting_key_transform)
            {
                for (size_t j = 0; j < input_order_info->order_key_prefix_descr.size(); ++j)
                    sort_description.emplace_back(metadata_snapshot->getSortingKey().column_names[j],
                                                  input_order_info->direction, 1);

                /// Drop temporary columns, added by 'sorting_key_prefix_expr'
                out_projection = createProjection(pipe, data);
                pipe.addSimpleTransform([sorting_key_prefix_expr](const Block & header)
                                        {
                                            return std::make_shared<ExpressionTransform>(header, sorting_key_prefix_expr);
                                        });
            }

            if (pipe.numOutputPorts() > 1)
            {
                pipe.addTransform(std::make_shared<MergingSortedTransform>(
                    pipe.getHeader(), pipe.numOutputPorts(), sort_description, max_block_size));
            }
        }
    }

    return Pipe::unitePipes(std::move(res));
}

}
