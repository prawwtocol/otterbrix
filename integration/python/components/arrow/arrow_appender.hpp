#pragma once

#include "arrow.hpp"

#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>

#include <cstdint>
#include <memory>
#include <vector>

namespace components::arrow {

    enum class ArrowOffsetSize : uint8_t { REGULAR, LARGE };

    struct ArrowOptions {
        bool use_list_view = false;
        ArrowOffsetSize offset_size = ArrowOffsetSize::REGULAR;
    };

    namespace appender {
        struct ArrowAppendData;
    }

    //! The ArrowAppender class can be used to incrementally construct an arrow array by appending data chunks into it
    class ArrowAppender {
    public:
        ArrowAppender(std::vector<types::complex_logical_type> types_p,
                      uint64_t initial_capacity,
                      ArrowOptions options = {});
        ~ArrowAppender();

    public:
        //! Append a data chunk to the underlying arrow array
        void Append(vector::data_chunk_t &input, uint64_t from, uint64_t to, uint64_t input_size);
        //! Returns the underlying arrow array
        ArrowArray Finalize();
        uint64_t RowCount() const;
        static void ReleaseArray(ArrowArray *array);
        static ArrowArray *FinalizeChild(const types::complex_logical_type &type,
                                         std::unique_ptr<appender::ArrowAppendData> append_data_p);
        static std::unique_ptr<appender::ArrowAppendData>
            InitializeChild(const types::complex_logical_type &type,
                            uint64_t capacity,
                            const ArrowOptions &options = {});
        static void AddChildren(appender::ArrowAppendData &data, uint64_t count);

    private:
        //! The types of the chunks that will be appended in
        std::vector<types::complex_logical_type> types;
        //! The root arrow append data
        std::vector<std::unique_ptr<appender::ArrowAppendData>> root_data;
        //! The total row count that has been appended
        uint64_t row_count = 0;
        //! Arrow-specific options (offset size, list view) propagated into every child
        ArrowOptions options;
    };
} // namespace components::arrow
