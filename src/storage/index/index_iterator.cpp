/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(int idx, int max_idx_per_page,  long long uuid, BufferPoolManager *bpm, LeafPage *start_page) 
    :  idx_(idx), max_idx_per_page_(max_idx_per_page), uuid_(uuid), bpm_(bpm), cur_page_(start_page){
  if(nullptr == start_page) {
    is_end_page_ = true;
  }
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  if(nullptr == bpm_ || nullptr == cur_page_) {
    return true;
  }
  return is_end_page_;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  // get the value?
  if(!is_end_page_) {
    return std::move(MappingType(std::move(cur_page_->KeyAt(idx_)), std::move(cur_page_->ValueAt(idx_))));
  }
  return std::move(MappingType(std::move(KeyType()), std::move(ValueType())));
}
INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  if (is_end_page_) {
    return *this;
  }
  if(++idx_ < max_idx_per_page_) {
    return *this;
  }
  page_id_t next_page = cur_page_->GetNextPageId();
  if (next_page == INVALID_PAGE_ID) {
    is_end_page_ = true;
    return *this;
  }
  idx_ = 0;
  WritePageGuard guard = bpm_->FetchPageWrite(next_page);
  cur_page_ = reinterpret_cast<LeafPage *>(guard.AsMut<LeafPage *>());
  max_idx_per_page_ = cur_page_->GetSize();
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
