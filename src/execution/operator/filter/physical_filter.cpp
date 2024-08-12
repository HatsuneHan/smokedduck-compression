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
		if (lineage_manager->compress){
			vector<char*> empty_vector;
			vector<idx_t> empty_vector_idx_t;
			active_log->compressed_filter_log.PushBack(empty_vector, empty_vector_idx_t, 0, result_count, active_lop->children[0]->out_start, 0);
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
			if (lineage_manager->compress){
//				sel_t* sel_copy = new sel_t[result_count];
//				std::copy(state.sel.data(), state.sel.data() + result_count, sel_copy);
				// print state.sel.data()
				std::cout << "Total result count: " << result_count << std::endl;
//				for (size_t i = 0; i < result_count; ++i) {
//					std::cout << state.sel.data()[i] << " ";
//				}
//				std::cout << std::endl;

				vector<char*> bitmap_vector;
				vector<idx_t> bitmap_sizes;

				if(result_count >= 32) {
					size_t bitmap_size = (STANDARD_VECTOR_SIZE + 7) / 8;
					unsigned char* bitmap = new unsigned char[bitmap_size];
					std::memset(bitmap, 0, bitmap_size);
					bool is_first = true;
					size_t prev_index = 0;

					for (size_t i = 0; i < result_count; ++i) {
						sel_t index = state.sel.data()[i];
						if (index >= STANDARD_VECTOR_SIZE) {
							throw std::runtime_error("Index out of range");
						}
						if (index > prev_index || is_first) {
							// ensure monotonicity
							bitmap[index / 8] |= (1 << (7 - index % 8));
							is_first = false;
						} else {
							// here we finish the current bitmap
							// compress the bitmap
							int max_compressed_size = duckdb_lz4::LZ4_compressBound(bitmap_size);
							char *compressed_bitmap = new char[max_compressed_size];
							int compressed_size =
							    duckdb_lz4::LZ4_compress_fast(reinterpret_cast<const char *>(bitmap),
							                                     compressed_bitmap, bitmap_size, max_compressed_size, 1);
							if (compressed_size <= 0) {
								throw std::runtime_error("Compression failed");
							}

							// resize bitmap
							char *compressed_bitmap_copy = new char[compressed_size];
							std::copy(compressed_bitmap, compressed_bitmap + compressed_size, compressed_bitmap_copy);
							bitmap_vector.push_back(compressed_bitmap_copy);
							bitmap_sizes.push_back(compressed_size);

							// destruction
							delete[] compressed_bitmap; // delete the original compressed bitmap
							delete[] bitmap;            // delete the original bitmap

							// create a new bitmap
							bitmap = new unsigned char[bitmap_size];
							std::memset(bitmap, 0, bitmap_size);

							bitmap[index / 8] |= (1 << (7 - index % 8));
						}

						prev_index = index;
					}

					// compress the last bitmap
					int max_compressed_size = duckdb_lz4::LZ4_compressBound(bitmap_size);
					char *compressed_bitmap = new char[max_compressed_size];
					int compressed_size =
					    duckdb_lz4::LZ4_compress_fast(reinterpret_cast<const char *>(bitmap), compressed_bitmap,
					                                     bitmap_size, max_compressed_size, 1);
					if (compressed_size <= 0) {
						throw std::runtime_error("Compression failed");
					}

					char *compressed_bitmap_copy = new char[compressed_size];
					std::copy(compressed_bitmap, compressed_bitmap + compressed_size, compressed_bitmap_copy);
					bitmap_vector.push_back(compressed_bitmap_copy);
					bitmap_sizes.push_back(compressed_size);

					// destruction
					delete[] compressed_bitmap; // delete the original compressed bitmap
					delete[] bitmap;            // delete the original bitmap

					active_log->compressed_filter_log.PushBack(bitmap_vector, bitmap_sizes, bitmap_vector.size(), result_count, active_lop->children[0]->out_start, 1);

					std::cout << "Original size: " << sizeof(sel_t)*result_count << std::endl;
					size_t tmp_compress_size = 0;
					size_t tmp_uncompressed_size = 0;
					for(size_t i = 0; i < bitmap_sizes.size(); i++){
						tmp_compress_size += bitmap_sizes[i];
						tmp_uncompressed_size += bitmap_size;
					}
					std::cout << "Compressed size: " << tmp_compress_size << std::endl;
					std::cout << "Uncompressed size: " << tmp_uncompressed_size << std::endl;
				} else {
					sel_t* sel_copy = new sel_t[result_count];
					std::copy(state.sel.data(), state.sel.data() + result_count, sel_copy);

					bitmap_vector.push_back(reinterpret_cast<char*>(sel_copy));
					vector<idx_t> size_vector;
					size_vector.push_back(result_count * sizeof(sel_t));

					active_log->compressed_filter_log.PushBack(bitmap_vector, size_vector, 1, result_count, active_lop->children[0]->out_start, 0);
				}

				active_log->SetLatestLSN({active_log->compressed_filter_log.size, 0});

			} else {
				unique_ptr<sel_t[]> sel_copy(new sel_t[result_count]);
				std::copy(state.sel.data(), state.sel.data() + result_count, sel_copy.get());
				active_log->filter_log.push_back({move(sel_copy), result_count, active_lop->children[0]->out_start});
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
