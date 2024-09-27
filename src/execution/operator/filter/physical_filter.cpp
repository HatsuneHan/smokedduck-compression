#include "duckdb/execution/operator/filter/physical_filter.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/parallel/thread_context.hpp"

#ifdef LINEAGE
#include "duckdb/execution/lineage/lineage_manager.hpp"
#include "lz4.hpp"
#endif

namespace duckdb {

PhysicalFilter::PhysicalFilter(vector<LogicalType> types, vector<unique_ptr<Expression>> select_list,
                               idx_t estimated_cardinality)
    : CachingPhysicalOperator(PhysicalOperatorType::FILTER, std::move(types), estimated_cardinality) {
	D_ASSERT(select_list.size() > 0);
	if (select_list.size() > 1) {
		// create a big AND out of the expressions
		auto conjunction = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
		for (auto &expr : select_list) {
			conjunction->children.push_back(std::move(expr));
		}
		expression = std::move(conjunction);
	} else {
		expression = std::move(select_list[0]);
	}
}

class FilterState : public CachingOperatorState {
public:
	explicit FilterState(ExecutionContext &context, Expression &expr)
	    : executor(context.client, expr), sel(STANDARD_VECTOR_SIZE) {
	}

	ExpressionExecutor executor;
	SelectionVector sel;

public:
	void Finalize(const PhysicalOperator &op, ExecutionContext &context) override {
		context.thread.profiler.Flush(op, executor, "filter", 0);
	}
};

unique_ptr<OperatorState> PhysicalFilter::GetOperatorState(ExecutionContext &context) const {
	return make_uniq<FilterState>(context, *expression);
}

OperatorResultType PhysicalFilter::ExecuteInternal(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                   GlobalOperatorState &gstate, OperatorState &state_p) const {
	auto &state = state_p.Cast<FilterState>();
	idx_t result_count = state.executor.SelectExpression(input, state.sel);
	if (result_count == input.size()) {
#ifdef LINEAGE
		if (lineage_manager->capture && active_log) {
			if (lineage_manager->compress) {
				auto start = std::chrono::high_resolution_clock::now();

				// one time loop
				vector<idx_t> empty_vector;
				vector<idx_t> empty_vector_idx_t;
				vector<idx_t> empty_vector_is_compressed;

				size_t test_sum = 0;
				for (idx_t i = 0; i < result_count; i++) {
					test_sum += state.sel.data()[i];
				}

				auto end = std::chrono::high_resolution_clock::now();
				std::chrono::duration<double> duration = end - start;

				active_log->strategy_time += duration.count();

				active_log->compressed_filter_log.PushBack(empty_vector, empty_vector_idx_t, empty_vector_is_compressed,
				                                           0, result_count, active_lop->children[0]->out_start, 0);
				active_log->SetLatestLSN({active_log->compressed_filter_log.size, 0});
			} else {
				active_log->filter_log.push_back({nullptr, result_count, active_lop->children[0]->out_start});
				active_log->SetLatestLSN({active_log->filter_log.size(), 0});
			}
		}
#endif
		// nothing was filtered: skip adding any selection vectors
		chunk.Reference(input);
	} else {
#ifdef LINEAGE
		if (lineage_manager->capture && active_log && result_count) {
			if (lineage_manager->compress) {

				auto start = std::chrono::high_resolution_clock::now();

				// one time compression
				vector<vector<idx_t>> result_vector = ChangeSelToBitMap(state.sel.data(), result_count, CompressionMethod::LZ4);

				vector<idx_t> &bitmap_vector = result_vector[0];
				vector<idx_t> &bitmap_sizes = result_vector[1];
				vector<idx_t> &bitmap_is_compressed = result_vector[2];
				vector<idx_t> &use_bitmap = result_vector[3];

				// one time decompression
				{
					size_t bitmap_num = bitmap_vector.size();
					size_t offset = 0;

					sel_t* sel_copy;

					if (bitmap_num) {
						if (use_bitmap[0]){
							sel_copy = new sel_t[result_count];
							size_t index = 0;

							for(size_t i = 0; i < bitmap_num; i++){

								unsigned char* decompressed_bitmap = DecompressBitmap(bitmap_sizes[i],
								                                                      bitmap_is_compressed[i],
								                                                      reinterpret_cast<unsigned char*>(bitmap_vector[i]));

								for (size_t j = 0; j < STANDARD_VECTOR_SIZE; ++j) {
									if (decompressed_bitmap[j / 8] & (1 << (7 - (j % 8)))) {
										sel_copy[index++] = static_cast<sel_t>(j) + offset; // PostProcess() here
									}
								}
							}
						}

						else {
							sel_copy = reinterpret_cast<sel_t*>(bitmap_vector[0]);
							for(size_t i = 0; i < result_count; i++){
								sel_copy[i] += offset;
							}
						}
					}
				}

				// one time loop
				size_t test_sum = 0;
				for (idx_t i = 0; i < result_count; i++) {
					test_sum += state.sel.data()[i];
				}

				auto end = std::chrono::high_resolution_clock::now();
				std::chrono::duration<double> duration = end - start;

				active_log->strategy_time += duration.count();

				size_t acc_size = 0;
				for (idx_t i = 0; i < bitmap_vector.size(); i++) {
					acc_size += bitmap_sizes[i];
				}
				active_log->strategy_size += acc_size;
				active_log->uncompressed_size += sizeof(sel_t) * result_count;

				std::cout << "Original size : " << sizeof(sel_t) * result_count << " Compressed size : " << acc_size << std::endl;

				active_log->compressed_filter_log.PushBack(bitmap_vector, bitmap_sizes, bitmap_is_compressed,
				                                           bitmap_vector.size(), result_count,
				                                           active_lop->children[0]->out_start, use_bitmap[0]);

				active_log->SetLatestLSN({active_log->compressed_filter_log.size, 0});

			} else {
				unique_ptr<sel_t[]> sel_copy(new sel_t[result_count]);
				std::copy(state.sel.data(), state.sel.data() + result_count, sel_copy.get());
				active_log->filter_log.push_back(
					{move(sel_copy), result_count, active_lop->children[0]->out_start});
				active_log->SetLatestLSN({active_log->filter_log.size(), 0});
			}
		}
#endif
		chunk.Slice(input, state.sel, result_count);
	}

	return OperatorResultType::NEED_MORE_INPUT;
}

string PhysicalFilter::ParamsToString() const {
	auto result = expression->GetName();
	result += "\n[INFOSEPARATOR]\n";
	result += StringUtil::Format("EC: %llu", estimated_cardinality);
	return result;
}

} // namespace duckdb
