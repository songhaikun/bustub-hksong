//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_delete_test.cpp
//
// Identification: test/storage/b_plus_tree_delete_test.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <cstdio>

#include "buffer/buffer_pool_manager.h"
#include "storage/disk/disk_manager_memory.h"
#include "storage/index/b_plus_tree.h"
#include "test_util.h" // NOLINT
#include "gtest/gtest.h"
#include <cstdlib>

namespace bustub {

using bustub::DiskManagerUnlimitedMemory;

TEST(BPlusTreeTests, DeleteTest1) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  // auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto disk_manager = std::make_unique<DiskManagerMemory>(256 << 10);
  // auto disk_manager = std::make_unique<DiskManager>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree(
      "foo_pk", header_page->GetPageId(), bpm, comparator);
  GenericKey<8> index_key;
  // RID rid;
  // create transaction
  auto *transaction = new Transaction(0);

  std::vector<int64_t> keys = {};
  for (int i = 0; i < 10000; i++) {
    keys.push_back(i);
  }
  std::vector<std::thread> thrds;
  for(int i = 0; i < 10; ++i) {
    std::thread t([i, &keys, &tree, transaction](){
      for(int j = i * 1000; j < (i + 1) * 1000; ++j){
        auto key = keys[j];
        int64_t value = key & 0xFFFFFFFF;
        RID rid;
        GenericKey<8> index_key;
        rid.Set(static_cast<int32_t>(key >> 32), value);
        index_key.SetFromInteger(key);
        tree.Insert(index_key, rid, transaction);
      }
    });
    thrds.push_back(std::move(t));
  }
  for (auto& t : thrds) {
    t.join();
  }
  std::cout << "finished" << std::endl;
  tree.Print(bpm);
  std::vector<RID> rids;

  // for (auto key : keys) {
  //   int64_t value = key & 0xFFFFFFFF;
  //   RID rid;
  //   GenericKey<8> index_key;
  //   rid.Set(static_cast<int32_t>(key >> 32), value);
  //   index_key.SetFromInteger(key);
  //   tree.Insert(index_key, rid, transaction);
  // }

  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }





  // for(auto key : remove_keys) {
  //   int64_t value = key & 0xFFFFFFFF;
  //   RID rid;
  //   GenericKey<8> index_key;
  //   rid.Set(static_cast<int32_t>(key >> 32), value);
  //   index_key.SetFromInteger(key);
  //   tree.Remove(index_key, transaction);
  // }
  // tree.Print(bpm);

  // std::vector<int64_t> remove_keys = {};
  // for (int i = 0; i < 10000; i++) {
  //   remove_keys.push_back(i);
  // }
  // std::vector<std::thread> del_threds;
  // for(int i = 0; i < 10; ++i) {
  //   std::thread t([i, &remove_keys, &tree, transaction](){
  //     for(int j = i * 1000; j < (i + 1) * 1000; ++j){
  //       std::cout << "remove:" << j << std::endl;
  //       auto key = remove_keys[j];
  //       GenericKey<8> index_key;
  //       index_key.SetFromInteger(key);
  //       if(j == 1250) {
  //         tree.Remove(index_key, transaction);
  //       }
  //       tree.Remove(index_key, transaction);
  //     }
  //   });
  //   del_threds.push_back(std::move(t));
  // }
  // for(auto& t : del_threds) {
  //   t.join();
  // }
  // tree.Print(bpm);
  // tree.Print(bpm);
  // for (int i = 0; i < 10000; i++) {
  //   keys.push_back(i);
  // }
  // std::cout << "insert start" << std::endl;
  // std::vector<std::thread> threds_2;
  // // std::vector<std::thread> thrds;
  // for(int i = 0; i < 10; ++i) {
  //   std::thread t([i, &keys, &tree, transaction](){
  //     for(int j = i * 1000; j < (i + 1) * 1000; ++j){
  //       auto key = keys[j];
  //       int64_t value = key & 0xFFFFFFFF;
  //       RID rid;
  //       GenericKey<8> index_key;
  //       rid.Set(static_cast<int32_t>(key >> 32), value);
  //       index_key.SetFromInteger(key);
  //       tree.Insert(index_key, rid, transaction);
  //     }
  //   });
  //   threds_2.push_back(std::move(t));
  // }
  // for(auto& t : threds_2) {
  //   t.join();
  // }

  // tree.Print(bpm);
  // std::vector<std::thread> thrds_1;
  // for(int i = 10; i < 15; ++i) {
  //   std::thread t([i, &keys, &tree, transaction](){
  //     for(int j = i * 1000; j < (i + 1) * 1000; ++j){
  //       auto key = keys[j];
  //       int64_t value = key & 0xFFFFFFFF;
  //       RID rid;
  //       GenericKey<8> index_key;
  //       rid.Set(static_cast<int32_t>(key >> 32), value);
  //       index_key.SetFromInteger(key);
  //       tree.Insert(index_key, rid, transaction);
  //     }
  //   });
  //   thrds_1.push_back(std::move(t));
  // }
  // for(auto& t : thrds_1) {
  //   t.join();
  // }

  // tree.Print(bpm);
  // std::cout << "remove end\n" << std::endl;
  // tree.Print(bpm);

  // for (auto key : remove_keys) {
  //   index_key.SetFromInteger(key);
  //   tree.Remove(index_key, transaction);
  // }
  // tree.Print(bpm);

  // int64_t size = 0;
  // bool is_present;

  // for (auto key : keys) {
  //   rids.clear();
  //   index_key.SetFromInteger(key);
  //   is_present = tree.GetValue(index_key, &rids);

  //   if (!is_present) {  // can't find
  //     EXPECT_NE(std::find(remove_keys.begin(), remove_keys.end(), key),
  //               remove_keys.end());
  //   } else {  // can find
  //     EXPECT_EQ(rids.size(), 1);
  //     EXPECT_EQ(rids[0].GetPageId(), 0);
  //     EXPECT_EQ(rids[0].GetSlotNum(), key);
  //     size = size + 1;
  //   }
  // }
  // std::cout << size << std::endl;

  // EXPECT_EQ(size, keys.size() - remove_keys.size());

  // bpm->UnpinPage(HEADER_PAGE_ID, true);
  // delete transaction;
  // delete bpm;
}

TEST(BPlusTreeTests, DeleteTest2) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree(
      "foo_pk", header_page->GetPageId(), bpm, comparator);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);

  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }

  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  std::vector<int64_t> remove_keys = {1, 5, 3, 4};
  for (auto key : remove_keys) {
    index_key.SetFromInteger(key);
    tree.Remove(index_key, transaction);
  }

  int64_t size = 0;
  bool is_present;

  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    is_present = tree.GetValue(index_key, &rids);

    if (!is_present) {
      EXPECT_NE(std::find(remove_keys.begin(), remove_keys.end(), key),
                remove_keys.end());
    } else {
      EXPECT_EQ(rids.size(), 1);
      EXPECT_EQ(rids[0].GetPageId(), 0);
      EXPECT_EQ(rids[0].GetSlotNum(), key);
      size = size + 1;
    }
  }

  EXPECT_EQ(size, 1);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete bpm;
}
} // namespace bustub
