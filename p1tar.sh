cd build && make format && make check-lint && make check-clang-tidy-p1 && cd .. && \
zip project1-submission.zip \
    src/include/container/hash/extendible_hash_table.h \
    src/container/hash/extendible_hash_table.cpp \
    src/include/buffer/lru_k_replacer.h \
    src/buffer/lru_k_replacer.cpp \
    src/include/buffer/buffer_pool_manager_instance.h \
    src/buffer/buffer_pool_manager_instance.cpp